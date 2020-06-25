CREATE TABLE `options2` (
`channel_id` BIGINT NOT NULL ,
`reply_prob` DOUBLE NULL DEFAULT NULL ,
`mention_only` TINYINT NULL DEFAULT NULL ,
`prefix_only` TINYINT NULL DEFAULT NULL ,
`ignore_channel` TINYINT NULL DEFAULT NULL ,
`extra_prefix` VARCHAR(255) NULL DEFAULT NULL ,
`max_bot_msg_length` INT NULL DEFAULT NULL ,
`reply_to_bots` TINYINT NULL DEFAULT NULL ,
`is_bad` TINYINT NULL DEFAULT NULL ,
`is_hidden` TINYINT NULL DEFAULT NULL ,
PRIMARY KEY (`channel_id`)) ENGINE = ROCKSDB; 

