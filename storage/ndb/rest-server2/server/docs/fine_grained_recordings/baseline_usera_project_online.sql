
/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
DROP TABLE IF EXISTS `kafka_offsets`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `kafka_offsets` (
  `consumer_group` varchar(255) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `topic` varchar(255) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,
  `partition` smallint NOT NULL,
  `offset` bigint unsigned NOT NULL,
  PRIMARY KEY (`consumer_group`,`topic`,`partition`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
/*!40101 SET character_set_client = @saved_cs_client */;

INSERT INTO `kafka_offsets` SET
  `consumer_group` = 'RONDB',
  `topic` = 'usera_project_onlinefs',
  `partition` = 3,
  `offset` = 2;
INSERT INTO `kafka_offsets` SET
  `consumer_group` = 'RONDB',
  `topic` = 'usera_project_onlinefs',
  `partition` = 7,
  `offset` = 2;
INSERT INTO `kafka_offsets` SET
  `consumer_group` = 'RONDB',
  `topic` = 'usera_project_onlinefs',
  `partition` = 1,
  `offset` = 2;
INSERT INTO `kafka_offsets` SET
  `consumer_group` = 'VECTORDB',
  `topic` = 'usera_project_onlinefs',
  `partition` = 1,
  `offset` = 2;
INSERT INTO `kafka_offsets` SET
  `consumer_group` = 'VECTORDB',
  `topic` = 'usera_project_onlinefs',
  `partition` = 7,
  `offset` = 2;
INSERT INTO `kafka_offsets` SET
  `consumer_group` = 'VECTORDB',
  `topic` = 'usera_project_onlinefs',
  `partition` = 8,
  `offset` = 2;
INSERT INTO `kafka_offsets` SET
  `consumer_group` = 'VECTORDB',
  `topic` = 'usera_project_onlinefs',
  `partition` = 3,
  `offset` = 2;
INSERT INTO `kafka_offsets` SET
  `consumer_group` = 'RONDB',
  `topic` = 'usera_project_onlinefs',
  `partition` = 8,
  `offset` = 2;
DROP TABLE IF EXISTS `online_ingestion_result`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `online_ingestion_result` (
  `online_ingestion_id` int NOT NULL,
  `status` tinyint(1) NOT NULL,
  `rows` bigint unsigned NOT NULL,
  PRIMARY KEY (`online_ingestion_id`,`status`),
  KEY `online_ingestion_id` (`online_ingestion_id`)
) ENGINE=ndbcluster DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;
/*!40101 SET character_set_client = @saved_cs_client */;

INSERT INTO `online_ingestion_result` SET
  `online_ingestion_id` = 100000,
  `status` = 0,
  `rows` = 4;
INSERT INTO `online_ingestion_result` SET
  `online_ingestion_id` = 100001,
  `status` = 0,
  `rows` = 4;
DROP TABLE IF EXISTS `usera_customers_fg_1`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `usera_customers_fg_1` (
  `customer_id` bigint NOT NULL,
  `age` bigint DEFAULT NULL,
  `country` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_premium` bigint DEFAULT NULL,
  `event_time` timestamp NULL /*!50606 STORAGE MEMORY */ DEFAULT NULL,
  PRIMARY KEY (`customer_id`) USING HASH
) ENGINE=ndbcluster DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='NDB_TABLE=READ_BACKUP=1';
/*!40101 SET character_set_client = @saved_cs_client */;

INSERT INTO `usera_customers_fg_1` SET
  `customer_id` = 2,
  `age` = 41,
  `country` = 'PK',
  `is_premium` = 1,
  `event_time` = '2026-07-16 08:53:35';
INSERT INTO `usera_customers_fg_1` SET
  `customer_id` = 4,
  `age` = 52,
  `country` = 'NO',
  `is_premium` = 0,
  `event_time` = '2026-07-16 08:53:35';
INSERT INTO `usera_customers_fg_1` SET
  `customer_id` = 3,
  `age` = 33,
  `country` = 'SE',
  `is_premium` = 1,
  `event_time` = '2026-07-16 08:53:35';
INSERT INTO `usera_customers_fg_1` SET
  `customer_id` = 1,
  `age` = 25,
  `country` = 'SE',
  `is_premium` = 0,
  `event_time` = '2026-07-16 08:53:35';
DROP TABLE IF EXISTS `usera_transactions_fg_1`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `usera_transactions_fg_1` (
  `customer_id` bigint NOT NULL,
  `num_transactions_30d` bigint DEFAULT NULL,
  `total_spend_30d` double DEFAULT NULL,
  `avg_transaction_value_30d` double DEFAULT NULL,
  `event_time` timestamp NULL /*!50606 STORAGE MEMORY */ DEFAULT NULL,
  PRIMARY KEY (`customer_id`) USING HASH
) ENGINE=ndbcluster DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='NDB_TABLE=READ_BACKUP=1';
/*!40101 SET character_set_client = @saved_cs_client */;

INSERT INTO `usera_transactions_fg_1` SET
  `customer_id` = 2,
  `num_transactions_30d` = 8,
  `total_spend_30d` = 540,
  `avg_transaction_value_30d` = 67.5,
  `event_time` = '2026-07-16 08:53:35';
INSERT INTO `usera_transactions_fg_1` SET
  `customer_id` = 4,
  `num_transactions_30d` = 1,
  `total_spend_30d` = 45.9,
  `avg_transaction_value_30d` = 45.9,
  `event_time` = '2026-07-16 08:53:35';
INSERT INTO `usera_transactions_fg_1` SET
  `customer_id` = 3,
  `num_transactions_30d` = 12,
  `total_spend_30d` = 880.2,
  `avg_transaction_value_30d` = 73.35,
  `event_time` = '2026-07-16 08:53:35';
INSERT INTO `usera_transactions_fg_1` SET
  `customer_id` = 1,
  `num_transactions_30d` = 3,
  `total_spend_30d` = 120.5,
  `avg_transaction_value_30d` = 40.17,
  `event_time` = '2026-07-16 08:53:35';
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

