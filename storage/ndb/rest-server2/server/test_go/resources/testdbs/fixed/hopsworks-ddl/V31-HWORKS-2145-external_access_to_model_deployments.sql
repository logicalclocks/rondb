CREATE TABLE IF NOT EXISTS `hopsworks`.`remote_user_group`
(
    `id`   INT(11) AUTO_INCREMENT PRIMARY KEY,
    `ruid` INT(11)      NOT NULL, -- remote user ID
    `name` varchar(255) NOT NULL,
    UNIQUE KEY (`ruid`, `name`),
    CONSTRAINT `rem_user_group_rem_user_fk` FOREIGN KEY (`ruid`) REFERENCES `remote_user` (`id`)
        ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster
  DEFAULT CHARSET = latin1
  COLLATE = latin1_general_cs;

CREATE TABLE IF NOT EXISTS `hopsworks`.`serving_remote_access`
(
    `id`                     INT(11) AUTO_INCREMENT PRIMARY KEY,
    `remote_user_group_name` varchar(255) NOT NULL,
    `serving_id`             INT(11)      NOT NULL,
    `granted_by`             INT(11)      NOT NULL, -- user ID
    `granted_at`             timestamp    NOT NULL,
    UNIQUE KEY (`remote_user_group_name`, `serving_id`),
    CONSTRAINT `serving_ext_access_serving_fk` FOREIGN KEY (`serving_id`) REFERENCES `serving` (`id`)
        ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster
  DEFAULT CHARSET = latin1
  COLLATE = latin1_general_cs;
