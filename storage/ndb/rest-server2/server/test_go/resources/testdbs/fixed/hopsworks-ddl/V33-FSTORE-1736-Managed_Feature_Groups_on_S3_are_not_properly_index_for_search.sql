ALTER TABLE `hopsworks`.`command_search_fs`
  CHANGE COLUMN `inode_id` `hopsworks_id` VARCHAR(100) COLLATE latin1_general_cs NOT NULL;

ALTER TABLE `hopsworks`.`command_search_fs_history`
  CHANGE COLUMN `inode_id` `hopsworks_id` VARCHAR(100) COLLATE latin1_general_cs NOT NULL;
