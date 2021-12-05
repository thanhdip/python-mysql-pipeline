CREATE SCHEMA `third_party_sales`

CREATE TABLE `third_party_sales`.`third_party_sales` (
    `ticket_id` INT NULL,
    `trans_date` DATE NULL,
    `event_id` INT NULL,
    `event_name` VARCHAR(50) NULL,
    `event_date` DATE NULL,
    `event_type` VARCHAR(10) NULL,
    `event_city` VARCHAR(20) NULL,
    `customer_id` INT NULL,
    `price` DECIMAL NULL,
    `num_tickets` INT NULL
);