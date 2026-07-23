ALTER TABLE `hopsworks`.`serving`
   ADD `scheduling_config`      varchar(2000) COLLATE latin1_general_cs        DEFAULT NULL;

CREATE TABLE IF NOT EXISTS `kube_priority_class` (
    `id`                    INT(11) AUTO_INCREMENT PRIMARY KEY,
    `name`                  VARCHAR(253) NOT NULL,
    UNIQUE KEY `name_idx` (`name`)
    ) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;

CREATE TABLE IF NOT EXISTS `kube_project_priority_class` (
    `id`                    INT(11) AUTO_INCREMENT PRIMARY KEY,
    `priority_class_id`     INT(11) NOT NULL,
    `project_id`            INT(11), -- NULL key used for default project settings
    `base`                  INT(0) NOT NULL DEFAULT 0,
    CONSTRAINT `priority_class_fkc` FOREIGN KEY (`priority_class_id`) REFERENCES `hopsworks`.`kube_priority_class` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `priority_class_project_fkc` FOREIGN KEY (`project_id`) REFERENCES `hopsworks`.`project` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
    ) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;

CREATE TABLE IF NOT EXISTS `kube_label` (
    `id`                    INT(11) AUTO_INCREMENT PRIMARY KEY,
    `name`                  VARCHAR(317) NOT NULL,
    `value`                 VARCHAR(63),
    UNIQUE KEY `name_value_idx` (`name`,`value`)
    ) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;

CREATE TABLE IF NOT EXISTS `kube_project_label` (
    `id`                    INT(11) AUTO_INCREMENT PRIMARY KEY,
    `label_id`              INT(11) NOT NULL,
    `project_id`            INT(11), -- NULL key used for default project settings
    `base`                  INT(0) NOT NULL DEFAULT 0,
    CONSTRAINT `label_fkc` FOREIGN KEY (`label_id`) REFERENCES `hopsworks`.`kube_label` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `label_project_fkc` FOREIGN KEY (`project_id`) REFERENCES `hopsworks`.`project` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
    ) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;