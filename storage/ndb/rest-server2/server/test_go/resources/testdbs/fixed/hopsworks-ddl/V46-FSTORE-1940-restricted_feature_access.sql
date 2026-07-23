CREATE TABLE IF NOT EXISTS `hopsworks`.`restricted_feature_group_access`
(
    `id`                    INT(11) AUTO_INCREMENT PRIMARY KEY,
    `feature_store`         INT(11) NOT NULL,
    `feature_group`         INT(11) NOT NULL,
    `granted_by`            INT(11) NOT NULL,
    `granted_on`            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `granted_to_user`       INT(11) NOT NULL,
    `can_access_entirely`   TINYINT(1) NOT NULL DEFAULT 1,
    CONSTRAINT `rfga_fs_fk` FOREIGN KEY (`feature_store`) REFERENCES `feature_store` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `rfga_fg_fk` FOREIGN KEY (`feature_group`) REFERENCES `feature_group` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `rfga_granted_by_fk` FOREIGN KEY (`granted_by`) REFERENCES `users` (`uid`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `rfga_granted_to_user_fk` FOREIGN KEY (`granted_to_user`) REFERENCES `users` (`uid`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster
DEFAULT CHARSET = latin1
COLLATE = latin1_general_cs;

CREATE TABLE IF NOT EXISTS `hopsworks`.`restricted_feature_access`
(
    `id`                                        INT(11) AUTO_INCREMENT PRIMARY KEY,
    `restricted_feature_group_access`           INT(11) NOT NULL,
    `feature`                                   VARCHAR(63) NOT NULL,
    `granted_by`                                INT(11) NOT NULL,
    `granted_on`                                TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `granted_to_user`                           INT(11) NOT NULL,
    CONSTRAINT `rfa_rfga_fk` FOREIGN KEY (`restricted_feature_group_access`) REFERENCES `restricted_feature_group_access` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `rfa_granted_by_fk` FOREIGN KEY (`granted_by`) REFERENCES `users` (`uid`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `rfa_granted_to_user_fk` FOREIGN KEY (`granted_to_user`) REFERENCES `users` (`uid`) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE = ndbcluster
DEFAULT CHARSET = latin1
COLLATE = latin1_general_cs;
