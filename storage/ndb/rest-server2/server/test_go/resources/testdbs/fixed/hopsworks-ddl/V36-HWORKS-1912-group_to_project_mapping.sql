ALTER TABLE `hopsworks`.`remote_group_project_mapping`
    ADD COLUMN `group_type` VARCHAR(45) NOT NULL DEFAULT 'LDAP';

ALTER TABLE `hopsworks`.`remote_group_project_mapping`
    CHANGE COLUMN `remote_group` `group_name` VARCHAR(256) NOT NULL;

ALTER TABLE `hopsworks`.`remote_group_project_mapping`
    RENAME TO `hopsworks`.`group_project_mapping`;

ALTER TABLE `hopsworks`.`group_project_mapping`
    DROP INDEX `index3` ,
    ADD UNIQUE INDEX `index3` (`group_name` ASC, `project` ASC, `group_type` ASC) VISIBLE;
