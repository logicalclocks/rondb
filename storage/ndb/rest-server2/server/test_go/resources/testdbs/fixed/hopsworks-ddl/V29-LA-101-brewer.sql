CREATE TABLE `hopsworks`.`brewer_chat` (
                            `id` int NOT NULL AUTO_INCREMENT,
                            `created` bigint NOT NULL,
                            `modified` bigint NOT NULL,
                            `title` varchar(1000) DEFAULT NULL,
                            `user` int NOT NULL,
                            `project` int NOT NULL,
                            PRIMARY KEY (`id`),
                            KEY `brewer_user` (`user`),
                            KEY `brewer_project` (`project`),
                            CONSTRAINT `brewer_project_fkc` FOREIGN KEY (`project`) REFERENCES `project` (`id`) ON DELETE CASCADE,
                            CONSTRAINT `brewer_user_fkc` FOREIGN KEY (`user`) REFERENCES `users` (`uid`) ON DELETE CASCADE
) ENGINE=ndbcluster AUTO_INCREMENT=2058 DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs;

ALTER TABLE `hopsworks`.`serving` MODIFY COLUMN `model_name` varchar(255) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL;
ALTER TABLE `hopsworks`.`serving` MODIFY COLUMN `model_version` int DEFAULT NULL;
ALTER TABLE `hopsworks`.`serving` MODIFY COLUMN `model_path` varchar(255) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL;
ALTER TABLE `hopsworks`.`serving` MODIFY COLUMN `model_framework` int DEFAULT NULL;
ALTER TABLE `hopsworks`.`serving` ADD COLUMN `serving_runtime` varchar(255) CHARACTER SET latin1 COLLATE latin1_general_cs DEFAULT NULL;
