use vector_lib::{
    NamedInternalEvent, counter,
    internal_event::{CounterName, InternalEvent, error_stage, error_type},
};

#[derive(Debug, NamedInternalEvent)]
pub struct MysqlCdcConnectionError<'a> {
    pub error: String,
    pub endpoint: &'a str,
}

impl InternalEvent for MysqlCdcConnectionError<'_> {
    fn emit(self) {
        error!(
            message = "MySQL replication connection error; reconnecting.",
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
pub struct MysqlCdcDecodeError<'a> {
    pub error: String,
    pub database: &'a str,
    pub table: &'a str,
}

impl InternalEvent for MysqlCdcDecodeError<'_> {
    fn emit(self) {
        error!(
            message = "Failed to decode MySQL row change.",
            error = %self.error,
            database = %self.database,
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
