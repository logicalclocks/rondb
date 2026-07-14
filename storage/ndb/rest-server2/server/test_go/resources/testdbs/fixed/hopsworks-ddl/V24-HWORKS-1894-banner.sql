CREATE TABLE IF NOT EXISTS `banner`
(
    `id`         int           NOT NULL AUTO_INCREMENT,
    `message`    varchar(255)  NOT NULL,
    `created_on` timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `active`     tinyint       NOT NULL DEFAULT '0',
    PRIMARY KEY (`id`)
) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;
