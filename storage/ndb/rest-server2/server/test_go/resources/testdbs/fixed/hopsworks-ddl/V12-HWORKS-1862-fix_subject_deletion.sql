ALTER TABLE `hopsworks`.`project_topics`
    DROP FOREIGN KEY subject_idx;

ALTER TABLE `hopsworks`.`project_topics`
    ADD CONSTRAINT `subject_idx` FOREIGN KEY (`subject_id`) REFERENCES `subjects` (`id`) ON DELETE SET NULL ON UPDATE NO ACTION;
