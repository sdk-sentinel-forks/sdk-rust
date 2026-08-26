use crate::{
    abstractions::OwnedMeteredSemPermit,
    protosext::ValidPollWFTQResponse,
    worker::{
        WorkflowSlotKind,
        client::WorkerClient,
        workflow::{
            CacheMissFetchReq, HistoryUpdate, NextPageReq, PermittedWFT,
            history_update::HistoryPaginator,
        },
    },
};
use futures_util::{
    FutureExt, Stream, StreamExt,
    future::BoxFuture,
    stream::{self, PollNext},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use temporalio_common::protos::{TaskToken, coresdk::WorkflowSlotInfo};
use tokio::sync::Notify;
use tracing::Span;

/// Transforms incoming validated WFTs and history fetching requests into [PermittedWFT]s ready
/// for application to workflow state
pub(super) struct WFTExtractor {}

pub(super) enum WFTExtractorOutput {
    NewWFT(PermittedWFT),
    FetchResult(
        PermittedWFT,
        // Field isn't read, but we need to hold on to it.
        #[allow(dead_code)] Arc<HistfetchRC>,
    ),
    NextPage {
        paginator: HistoryPaginator,
        update: HistoryUpdate,
        span: Span,
        rc: Arc<HistfetchRC>,
    },
    FailedFetch {
        run_id: String,
        err: tonic::Status,
        auto_reply_fail_tt: Option<TaskToken>,
    },
    PollerDead,
}

enum BufferedOutput {
    WFT(Result<WFTExtractorOutput, tonic::Status>),
    Fetch(Result<WFTExtractorOutput, tonic::Status>),
    PollerDead,
}

pub(crate) type WFTStreamIn = Result<
    (
        ValidPollWFTQResponse,
        OwnedMeteredSemPermit<WorkflowSlotKind>,
    ),
    tonic::Status,
>;
#[derive(derive_more::From, Debug)]
pub(super) enum HistoryFetchReq {
    Full(Box<CacheMissFetchReq>, Arc<HistfetchRC>),
    NextPage(Box<NextPageReq>, Arc<HistfetchRC>),
}
/// Used inside of `Arc`s to ensure we don't shutdown while there are outstanding fetches.
#[derive(Debug)]
pub(super) struct HistfetchRC {}

impl WFTExtractor {
    pub(super) fn build(
        client: Arc<dyn WorkerClient>,
        max_fetch_concurrency: usize,
        wft_stream: impl Stream<Item = WFTStreamIn> + Send + 'static,
        fetch_stream: impl Stream<Item = HistoryFetchReq> + Send + 'static,
    ) -> impl Stream<Item = Result<WFTExtractorOutput, tonic::Status>> + Send + 'static {
        let fetch_client = client.clone();
        // Poller shutdown must wait for every earlier task to leave the unordered buffer. The
        // workflow stream processes each yielded task synchronously before polling this stream
        // again, so the shutdown marker cannot overtake a task after its counter is decremented.
        let pending_wft_outputs = Arc::new(AtomicUsize::new(0));
        let wft_output_delivered = Arc::new(Notify::new());
        let pending_wft_outputs_for_tasks = pending_wft_outputs.clone();
        let pending_wft_outputs_for_shutdown = pending_wft_outputs.clone();
        let wft_output_delivered_for_shutdown = wft_output_delivered.clone();
        let wft_stream = wft_stream
            .map(move |stream_in| {
                pending_wft_outputs_for_tasks.fetch_add(1, Ordering::Relaxed);
                let client = client.clone();
                async move {
                    BufferedOutput::WFT(match stream_in {
                        Ok((wft, permit)) => {
                            let run_id = wft.workflow_execution.run_id.clone();
                            let tt = wft.task_token.clone();
                            Ok(match HistoryPaginator::from_poll(wft, client).await {
                                Ok((pag, prep)) => WFTExtractorOutput::NewWFT(PermittedWFT {
                                    permit: permit.into_used(WorkflowSlotInfo {
                                        workflow_type: prep.workflow_type.clone(),
                                        is_sticky: prep.is_incremental(),
                                    }),
                                    work: prep,
                                    paginator: pag,
                                }),
                                Err(err) => WFTExtractorOutput::FailedFetch {
                                    run_id,
                                    err,
                                    auto_reply_fail_tt: Some(tt),
                                },
                            })
                        }
                        Err(e) => Err(e),
                    })
                }
                .right_future::<BoxFuture<'static, BufferedOutput>>()
                .left_future()
            })
            .chain(stream::iter([async move {
                loop {
                    let delivered = wft_output_delivered_for_shutdown.notified();
                    if pending_wft_outputs_for_shutdown.load(Ordering::Acquire) == 0 {
                        break;
                    }
                    delivered.await;
                }
                BufferedOutput::PollerDead
            }
            .boxed()
            .left_future()
            .left_future()]));

        stream::select_with_strategy(
            wft_stream,
            fetch_stream.map(move |fetchreq: HistoryFetchReq| {
                let client = fetch_client.clone();
                async move {
                    BufferedOutput::Fetch(Ok(match fetchreq {
                        // It's OK to simply drop the refcounters in the event of fetch
                        // failure. We'll just proceed with shutdown.
                        HistoryFetchReq::Full(req, rc) => {
                            let run_id = req.original_wft.work.execution.run_id.clone();
                            let task_token = req.original_wft.work.task_token.clone();
                            match HistoryPaginator::from_fetchreq(req, client).await {
                                Ok(r) => WFTExtractorOutput::FetchResult(r, rc),
                                Err(err) => WFTExtractorOutput::FailedFetch {
                                    run_id,
                                    err,
                                    auto_reply_fail_tt: Some(task_token),
                                },
                            }
                        }
                        HistoryFetchReq::NextPage(mut req, rc) => {
                            match req.paginator.extract_next_update().await {
                                Ok(update) => WFTExtractorOutput::NextPage {
                                    paginator: req.paginator,
                                    update,
                                    span: req.span,
                                    rc,
                                },
                                Err(err) => WFTExtractorOutput::FailedFetch {
                                    run_id: req.paginator.run_id,
                                    err,
                                    auto_reply_fail_tt: None,
                                },
                            }
                        }
                    }))
                }
                .right_future()
            }),
            // Priority always goes to the fetching stream
            |_: &mut ()| PollNext::Right,
        )
        .buffer_unordered(max_fetch_concurrency)
        .map(move |output| match output {
            BufferedOutput::WFT(output) => {
                if pending_wft_outputs.fetch_sub(1, Ordering::AcqRel) == 1 {
                    wft_output_delivered.notify_waiters();
                }
                output
            }
            BufferedOutput::Fetch(output) => output,
            BufferedOutput::PollerDead => Ok(WFTExtractorOutput::PollerDead),
        })
    }
}
