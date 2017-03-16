CREATE TABLE IF NOT EXISTS batch_job_request (
    job_seq_id bigserial PRIMARY KEY,
    job_name varchar(100) NOT NULL,
    job_parameter varchar(200),
    job_execution_id bigint,
    polling_status varchar(10) NOT NULL,
    create_date timestamp NOT NULL,
    update_date timestamp
);
