use vector_lib::{
    NamedInternalEvent, counter,
    internal_event::{CounterName, InternalEvent, error_stage, error_type},
};

#[derive(Debug, NamedInternalEvent)]
pub struct PostgresCdcConnectionError<'a> {
    pub error: String,
    pub endpoint: &'a str,
}

impl InternalEvent for PostgresCdcConnectionError<'_> {
    fn emit(self) {
        error!(
            message = "PostgreSQL replication connection error; reconnecting.",
            error = %self.error,
            endpoint = %self.endpoint,
            error_type = error_type::CONNECTION_FAILED,
            stage = error_stage::RECEIVING,
        );
        counter!(
            CounterName::ComponentErrorsTotal,
            "error_type" => error_type::CONNECTION_FAILED,
            "stage" => error_stage::RECEIVING,
        )
        .increment(1);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct PostgresCdcDecodeError<'a> {
    pub error: String,
    pub schema: &'a str,
    pub table: &'a str,
}

impl InternalEvent for PostgresCdcDecodeError<'_> {
    fn emit(self) {
        error!(
            message = "Failed to decode PostgreSQL row change.",
            error = %self.error,
            schema = %self.schema,
            table = %self.table,
            error_type = error_type::PARSER_FAILED,
            stage = error_stage::PROCESSING,
        );
        counter!(
            CounterName::ComponentErrorsTotal,
            "error_type" => error_type::PARSER_FAILED,
            "stage" => error_stage::PROCESSING,
        )
        .increment(1);
    }
}
