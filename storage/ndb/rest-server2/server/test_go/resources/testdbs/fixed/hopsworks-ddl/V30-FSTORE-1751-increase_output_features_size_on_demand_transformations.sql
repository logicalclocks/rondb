-- Increasing size of output features to hold at least 500 features of of 40 characters each. 
--          size : (500 * 42 = 22000), 2 added for joining the names. 
ALTER TABLE `hopsworks`.`feature_group_transformation_functions` 
    MODIFY COLUMN `output_features` VARCHAR(21000);

-- Increasing size of output type to hold at least 1000 type of of 9 characters each. The longest possible output type is 9 characters long.
--          size : (1000 * 11 = 11000), 2 added for joining the names. 
ALTER TABLE `hopsworks`.`transformation_function`
   MODIFY COLUMN `output_type` VARCHAR(11000);
