ALTER TABLE `hopsworks`.`training_dataset_feature`
    DROP FOREIGN KEY odtf_fk_fgtf;

ALTER TABLE `hopsworks`.`training_dataset_feature`
    ADD CONSTRAINT `odtf_fk_fgtf` FOREIGN KEY (`on_demand_transformation`) REFERENCES `feature_group_transformation_functions` (`id`) ON DELETE CASCADE ON UPDATE NO ACTION;
