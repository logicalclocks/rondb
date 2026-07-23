UPDATE `hopsworks`.`transformation_function` 
   SET execution_mode = "PANDAS" 
   WHERE execution_mode IS NULL;
