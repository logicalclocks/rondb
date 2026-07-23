CREATE TABLE IF NOT EXISTS `kubernetes_ops` (
    `id`                  INT(10) AUTO_INCREMENT PRIMARY KEY,
    `criticality`         VARCHAR(50) NOT NULL,
    `status`              VARCHAR(50) DEFAULT "New",
    `kind`                VARCHAR(100) NOT NULL,
    `verb`                VARCHAR(100) NOT NULL,
    `namespace`           VARCHAR(255) NOT NULL,
    `resource`            TEXT NOT NULL,
    `created_at`          DATETIME NOT NULL,
    INDEX `status` (`status`),
    INDEX `created_at` (`created_at`)
) ENGINE = ndbcluster DEFAULT CHARSET = latin1 COLLATE = latin1_general_cs;
