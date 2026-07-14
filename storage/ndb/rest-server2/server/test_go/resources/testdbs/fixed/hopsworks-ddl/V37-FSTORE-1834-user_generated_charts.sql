CREATE TABLE IF NOT EXISTS `hopsworks`.`chart` (
  `id` INT(11) AUTO_INCREMENT PRIMARY KEY,
  `title` VARCHAR(128) NOT NULL,
  `description` VARCHAR(1000) DEFAULT NULL,
  `url` VARCHAR(256) NOT NULL,
  `job_id` INT(11) DEFAULT NULL,
  `project_id` INT(11) NOT NULL,
  CONSTRAINT `chart_project_fk` FOREIGN KEY (`project_id`) REFERENCES `project` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
  CONSTRAINT `chart_job_fk` FOREIGN KEY (`job_id`) REFERENCES `jobs` (`id`) ON DELETE SET NULL ON UPDATE NO ACTION
) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;

CREATE TABLE IF NOT EXISTS `hopsworks`.`dashboard` (
  `id` INT(11) AUTO_INCREMENT PRIMARY KEY,
  `name` VARCHAR(128) NOT NULL,
  `project_id` INT(11) NOT NULL,
  CONSTRAINT `dashboard_project_fk` FOREIGN KEY (`project_id`) REFERENCES `project` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;

CREATE TABLE IF NOT EXISTS `hopsworks`.`dashboard_chart` (
  `dashboard_id` INT(11) NOT NULL,
  `chart_id` INT(11) NOT NULL,
  `width` INT(11) NOT NULL,
  `height` INT(11) NOT NULL,
  `x` INT(11) NOT NULL,
  `y` INT(11) NOT NULL,
  PRIMARY KEY (`dashboard_id`, `chart_id`),
  CONSTRAINT `dashboard_fk` FOREIGN KEY (`dashboard_id`) REFERENCES `dashboard` (`id`) ON DELETE CASCADE,
  CONSTRAINT `chart_fk` FOREIGN KEY (`chart_id`) REFERENCES `chart` (`id`) ON DELETE CASCADE
) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;
