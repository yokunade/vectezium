//! Internal events of the shared CDC core (`sources::cdc`), used by every CDC connector.

use vector_lib::{
    NamedInternalEvent, counter,
    internal_event::{CounterName, InternalEvent, error_stage, error_type},
};

#[derive(Debug, NamedInternalEvent)]
pub struct CdcCheckpointWriteError<'a> {
    pub error: std::io::Error,
    pub path: &'a str,
}

impl InternalEvent for CdcCheckpointWriteError<'_> {
    fn emit(self) {
        error!(
            message = "Failed to write CDC checkpoint.",
            error = %self.error,
            path = %self.path,
            error_type = error_type::WRITER_FAILED,
            stage = error_stage::PROCESSING,
        );
        counter!(
            CounterName::ComponentErrorsTotal,
            "error_type" => error_type::WRITER_FAILED,
            "stage" => error_stage::PROCESSING,
        )
        .increment(1);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct CdcDeliveryFailed {
    pub status: String,
}

impl InternalEvent for CdcDeliveryFailed {
    fn emit(self) {
        error!(
            message = "A batch of change events was not delivered; the source stops advancing its \
                       checkpoint and will shut down so that the changes are replayed after a restart.",
            status = %self.status,
            error_type = error_type::ACKNOWLEDGMENT_FAILED,
            stage = error_stage::SENDING,
        );
        counter!(
            CounterName::ComponentErrorsTotal,
            "error_type" => error_type::ACKNOWLEDGMENT_FAILED,
            "stage" => error_stage::SENDING,
        )
        .increment(1);
    }
}
