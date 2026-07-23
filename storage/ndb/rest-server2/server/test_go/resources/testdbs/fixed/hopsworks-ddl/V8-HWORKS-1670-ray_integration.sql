ALTER TABLE `hopsworks`.`executions` ADD COLUMN `ray_dashboard_url` varchar(255) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL;
ALTER TABLE `hopsworks`.`executions` ADD COLUMN `ray_cluster_name` varchar(255) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL;
ALTER TABLE `hopsworks`.`jupyter_settings` ADD COLUMN `ray_config` varchar(5000) COLLATE latin1_general_cs DEFAULT NULL;

CREATE TABLE `hopsworks`.`jupyter_ray_session` (
                                       `id` int NOT NULL AUTO_INCREMENT,
                                       `creation_time` bigint DEFAULT NULL,
                                       `jupyter_project_id` int NOT NULL,
                                       `application_id` varchar(255) NOT NULL,
                                       `kernel_id` varchar(255) NOT NULL,
                                       `ray_head_node_service` varchar(255) DEFAULT NULL,
                                       PRIMARY KEY (`id`),
                                       UNIQUE KEY `kernel_id_unique` (`kernel_id`),
                                       UNIQUE KEY `application_id_unique` (`application_id`),
                                       KEY `fk_ray_session_jupyter_project` (`jupyter_project_id`),
                                       CONSTRAINT `fk_ray_session_jupyter_project` FOREIGN KEY (`jupyter_project_id`) REFERENCES `jupyter_project` (`id`) ON DELETE CASCADE
) ENGINE=ndbcluster
  DEFAULT CHARSET=latin1
  COLLATE=latin1_general_cs;