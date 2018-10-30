SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";

CREATE TABLE IF NOT EXISTS `bots` (
  `id` bigint(20) NOT NULL,
  `date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS `chat` (
  `id` int(11) NOT NULL,
  `date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `server_id` bigint(20) DEFAULT NULL,
  `server_name` text COLLATE utf8mb4_bin,
  `channel_id` bigint(20) NOT NULL,
  `channel_name` text COLLATE utf8mb4_bin,
  `user_id` bigint(20) NOT NULL,
  `user_name` text COLLATE utf8mb4_bin NOT NULL,
  `message` text COLLATE utf8mb4_bin NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

DELIMITER //
CREATE TRIGGER `chat_insert` AFTER INSERT ON `chat`
 FOR EACH ROW BEGIN
INSERT INTO `chat_counters` (server_id, channel_id, user_id, message_count) VALUES (NEW.server_id, NEW.channel_id, NEW.user_id, 1) ON DUPLICATE KEY UPDATE message_count = message_count + 1;
END
//
DELIMITER ;

CREATE TABLE IF NOT EXISTS `chat_counters` (
  `server_id` bigint(20) DEFAULT NULL,
  `user_id` bigint(20) NOT NULL,
  `channel_id` bigint(20) NOT NULL,
  `message_count` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS `mentions` (
  `user_id` bigint(20) NOT NULL,
  `name` varchar(64) COLLATE utf8mb4_bin NOT NULL,
  `mention` varchar(64) COLLATE utf8mb4_bin NOT NULL,
  `date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `counter` int(11) NOT NULL DEFAULT '1'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS `options` (
  `convid` bigint(20) NOT NULL,
  `option` varchar(32) COLLATE utf8mb4_bin NOT NULL,
  `value` varchar(32) COLLATE utf8mb4_bin NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

ALTER TABLE `bots`
 ADD PRIMARY KEY (`id`);

ALTER TABLE `chat`
 ADD PRIMARY KEY (`id`), ADD UNIQUE KEY `channel_id` (`channel_id`,`id`), ADD FULLTEXT KEY `message` (`message`);

ALTER TABLE `chat_counters`
 ADD PRIMARY KEY (`user_id`,`channel_id`);

ALTER TABLE `mentions`
 ADD PRIMARY KEY (`name`,`mention`);

ALTER TABLE `options`
 ADD PRIMARY KEY (`convid`,`option`);

ALTER TABLE `chat`
MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
