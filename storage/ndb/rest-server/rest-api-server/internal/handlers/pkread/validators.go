package pkread

import (
	"errors"
	"fmt"

	"hopsworks.ai/rdrs/internal/handlers/validators"
	"hopsworks.ai/rdrs/pkg/api"
)

// TODO: Add nil pointer checks
func ValidateBody(params *api.PKReadParams) error {
	for _, filter := range *params.Filters {
		// make sure filter columns are valid
		if err := validators.ValidateDBIdentifier(filter.Column); err != nil {
			return err
		}
	}

	// make sure that the columns are unique.
	existingFilters := make(map[string]bool)
	for _, filter := range *params.Filters {
		if _, ok := existingFilters[*filter.Column]; ok {
			return fmt.Errorf("field validation for filter failed on the 'unique' tag")
		} else {
			existingFilters[*filter.Column] = true
		}
	}

	// make sure read columns are valid
	if params.ReadColumns != nil {
		for _, col := range *params.ReadColumns {
			if err := validators.ValidateDBIdentifier(col.Column); err != nil {
				return err
			}
		}
	}

	// make sure that the filter columns and read colummns do not overlap
	// and read cols are unique
	if params.ReadColumns != nil {
		existingCols := make(map[string]bool)
		for _, readCol := range *params.ReadColumns {
			if _, ok := existingFilters[*readCol.Column]; ok {
				return fmt.Errorf("field validation for read columns faild. '%s' already included in filter", *readCol.Column)
			}

			if _, ok := existingCols[*readCol.Column]; ok {
				return errors.New("field validation for 'ReadColumns' failed on the 'unique' tag")
			} else {
				existingCols[*readCol.Column] = true
			}
		}
	}

	return nil
}
