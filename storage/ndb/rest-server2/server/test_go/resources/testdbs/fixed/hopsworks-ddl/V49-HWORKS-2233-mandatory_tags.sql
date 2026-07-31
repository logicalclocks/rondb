CREATE TABLE IF NOT EXISTS `hopsworks`.`feature_store_mandatory_tag`
(
    `id`          INT(11) AUTO_INCREMENT PRIMARY KEY,
    `tag_id`      INT(11) NOT NULL,
    `project_id`  INT(11) NULL,  -- NULL for cluster-wide tags
    `fg_specific` BOOLEAN       DEFAULT FALSE,
    `fv_specific` BOOLEAN       DEFAULT FALSE,
    `td_specific` BOOLEAN       DEFAULT FALSE,
    CONSTRAINT `mandatory_tags_tag_fk` FOREIGN KEY (`tag_id`) REFERENCES `feature_store_tag` (`id`)
    ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `mandatory_tags_project_fk` FOREIGN KEY (`project_id`) REFERENCES `project` (`id`)
    ON DELETE CASCADE ON UPDATE NO ACTION,
    INDEX `idx_mandatory_tag_project` (`project_id`)  -- small table; additional indices unnecessary
    ) ENGINE = ndbcluster
    DEFAULT CHARSET = latin1
    COLLATE = latin1_general_cs;
