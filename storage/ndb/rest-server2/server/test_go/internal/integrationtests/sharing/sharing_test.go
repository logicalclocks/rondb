/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2026 Hopsworks AB
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

// Fine-grained FG/FV sharing tests (RONDB-1088).
//
// Fixture state: fixed/hopsworks-data/fine_grained_sharing_data.sql - a
// cleaned import of a live Hopsworks 5.0.1 cluster. The scenario ids (A1,
// B2, C1, D0, ...) and the expected outcomes are pinned by the reference
// recordings in docs/fine_grained_recordings/ and the E-rules of
// docs/fg_fv_sharing_fine_grained_test_design.md par.2/2.5:
//
//   usera  owner of usera_project (FGs usera_customers_fg_1 +
//          usera_transactions_fg_1; FVs usera_customers_transactions_fv
//          + usera_txncount_fv)
//   userb  usera's store shared ENTIRELY with userb_project        (A)
//   userc  both FGs shared whole (+ own cross-store userc_own_fv)  (B3/F1)
//   userd  customers_fg shared whole, nothing else                 (B1/B2/B4)
//   usere  customers whole + transactions subset
//          {customer_id, event_time, num_transactions_30d}
//          (+ own usere_own_fv over granted columns)               (C/F2)
//   userf  no grants at all - outsider                             (A2)
//   userj  'Feature store restricted' member of usera_project,
//          NO grants - must see nothing, not even the member DB    (D0)
//   userk  restricted, customers_fg granted entirely
//          (+ userk_own_fv in usera_project)                       (D2/F4)
//   userl  restricted, customers entirely + transactions subset
//          {customer_id, event_time, num_transactions_30d}
//          (+ userl_own_fv in usera_project)                       (D3/F5)
//
// Denial contract (design doc par.2.5, decision D3 refined): HTTP 401 with
// a message naming the blocking object - the database for store-level
// denials (existing behavior), the table for table-level denials and the
// column(s) for column-level denials, mirroring MySQL errors 1044/1142/1143
// which are how Hopsworks denies these reads online.
//
// Data values of allowed reads are validated against mysqld directly
// (readRowFromMySQL) - the recordings only pin the authorization contract.
//
// The scan endpoint shares the same authorization path (table+column
// checks in validate_api_key); it has no Go test here because the FG
// tables have hash-only primary keys, which index-scan cannot read. The
// C++ api_key_test covers the scan-shaped authorization requests.

package sharing

import (
	"fmt"
	"net/http"
	"testing"

	"hopsworks.ai/rdrs2/internal/integrationtests/batchfeaturestore"
	"hopsworks.ai/rdrs2/internal/integrationtests/feature_store"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/pkg/api"
	"hopsworks.ai/rdrs2/resources/testdbs"
)

const (
	useraProject      = "usera_project"
	customersTable    = "usera_customers_fg_1"
	transactionsTable = "usera_transactions_fg_1"

	fullFV   = "usera_customers_transactions_fv"
	narrowFV = "usera_txncount_fv"

	// denial-message fragments (the sharing denial contract)
	denyDB        = "not authorized to access " + useraProject
	denyUsercDB   = "not authorized to access userc_project"
	denyUsereDB   = "not authorized to access usere_project"
	denyTxTable   = useraProject + "/" + transactionsTable
	denyCustTable = useraProject + "/" + customersTable
	denySpendCol  = "total_spend_30d"
)

// feature column sets, in the FVs' serving order (training_dataset_feature.idx)
var (
	fullFVCustomersCols    = []string{"customer_id", "age", "country", "is_premium"}
	fullFVTransactionsCols = []string{"num_transactions_30d", "total_spend_30d", "avg_transaction_value_30d"}

	narrowFVCustomersCols    = []string{"customer_id", "age"}
	narrowFVTransactionsCols = []string{"num_transactions_30d"}
)

// expectedVector reads the FV's constituent columns straight from mysqld
// and concatenates them in serving order.
func expectedVector(t *testing.T, customerID int, customersCols []string,
	transactionsCols []string) []string {
	expected := readRowFromMySQL(t, useraProject, customersTable, customerID, customersCols)
	if transactionsCols != nil {
		expected = append(expected,
			readRowFromMySQL(t, useraProject, transactionsTable, customerID, transactionsCols)...)
	}
	return expected
}

type fvReadTest struct {
	scenario string // recording / design-doc scenario id
	apiKey   string
	fsName   string
	fvName   string
	httpCode int
	// substring the denial body must contain (denial contract)
	errMsgContains string
	// on 200: columns to validate against mysqld, split per source table
	customersCols    []string
	transactionsCols []string
}

// The FV serving matrix - every user key against the producer FVs and the
// consumer-created FVs. This one table drives both the single and the
// batch feature-store endpoint tests.
var fvReadTests = []fvReadTest{
	// owner
	// usera serves his own 7-feature FV via plain membership - baseline
	// proving the grant machinery does not break the owner's access.
	{
		scenario:         "owner_full_fv",
		apiKey:           testdbs.USERA_API_KEY,
		fsName:           useraProject,
		fvName:           fullFV,
		httpCode:         http.StatusOK,
		customersCols:    fullFVCustomersCols,
		transactionsCols: fullFVTransactionsCols,
	},
	// Same for the 3-feature narrow FV.
	{
		scenario:         "owner_narrow_fv",
		apiKey:           testdbs.USERA_API_KEY,
		fsName:           useraProject,
		fvName:           narrowFV,
		httpCode:         http.StatusOK,
		customersCols:    narrowFVCustomersCols,
		transactionsCols: narrowFVTransactionsCols,
	},

	// A1: store shared entirely -> everything serves
	// usera's store is shared ENTIRELY with userb_project: the full FV
	// serves although userb is no member of usera_project.
	{
		scenario:         "A1_userb_full_fv",
		apiKey:           testdbs.USERB_API_KEY,
		fsName:           useraProject,
		fvName:           fullFV,
		httpCode:         http.StatusOK,
		customersCols:    fullFVCustomersCols,
		transactionsCols: fullFVTransactionsCols,
	},
	// The store-entirely grant covers every FV of the store.
	{
		scenario:         "A1_userb_narrow_fv",
		apiKey:           testdbs.USERB_API_KEY,
		fsName:           useraProject,
		fvName:           narrowFV,
		httpCode:         http.StatusOK,
		customersCols:    narrowFVCustomersCols,
		transactionsCols: narrowFVTransactionsCols,
	},

	// A2: outsider -> store-level denial on every surface
	// userf holds no grant of any kind: denied at the database level,
	// before any table/column logic.
	{
		scenario:       "A2_userf_full_fv",
		apiKey:         testdbs.USERF_API_KEY,
		fsName:         useraProject,
		fvName:         fullFV,
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denyDB,
	},
	// Outsider denial is independent of which FV is asked for.
	{
		scenario:       "A2_userf_narrow_fv",
		apiKey:         testdbs.USERF_API_KEY,
		fsName:         useraProject,
		fvName:         narrowFV,
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denyDB,
	},

	// B3: both FGs shared whole -> full FV serves
	// No store share for userc - two whole-FG grants alone are enough to
	// serve the FV that joins them.
	{
		scenario:         "B3_userc_full_fv",
		apiKey:           testdbs.USERC_API_KEY,
		fsName:           useraProject,
		fvName:           fullFV,
		httpCode:         http.StatusOK,
		customersCols:    fullFVCustomersCols,
		transactionsCols: fullFVTransactionsCols,
	},
	// Whole-FG grants also cover any feature subset an FV selects.
	{
		scenario:         "B3_userc_narrow_fv",
		apiKey:           testdbs.USERC_API_KEY,
		fsName:           useraProject,
		fvName:           narrowFV,
		httpCode:         http.StatusOK,
		customersCols:    narrowFVCustomersCols,
		transactionsCols: narrowFVTransactionsCols,
	},
	// F1: consumer-created cross-store FV in userc's own store
	// The FV's metadata lives in userc_project while its data tables live
	// in usera_project - the cross-store serving path.
	{
		scenario:         "F1_userc_own_fv",
		apiKey:           testdbs.USERC_API_KEY,
		fsName:           "userc_project",
		fvName:           "userc_own_fv",
		httpCode:         http.StatusOK,
		customersCols:    fullFVCustomersCols,
		transactionsCols: fullFVTransactionsCols,
	},

	// B2: only customers_fg shared -> FVs needing transactions_fg denied,
	// message names the missing table
	// One granted FG does not unlock an FV that joins a second, unshared
	// FG (the FSTORE-2026 partial-FG case).
	{
		scenario:       "B2_userd_full_fv",
		apiKey:         testdbs.USERD_API_KEY,
		fsName:         useraProject,
		fvName:         fullFV,
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denyTxTable,
	},
	// The narrow FV also selects one transactions column, so it is just as
	// blocked - userd has no transactions grant at all.
	{
		scenario:       "B2_userd_narrow_fv",
		apiKey:         testdbs.USERD_API_KEY,
		fsName:         useraProject,
		fvName:         narrowFV,
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denyTxTable,
	},

	// C1/C2: customers whole + transactions{num_transactions_30d}
	// The full FV needs total_spend_30d/avg_transaction_value_30d, which
	// are outside usere's column subset - denial names the columns.
	{
		scenario:       "C2_usere_full_fv",
		apiKey:         testdbs.USERE_API_KEY,
		fsName:         useraProject,
		fvName:         fullFV,
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denySpendCol,
	},
	// The narrow FV's only transactions column IS in the subset -> serves.
	{
		scenario:         "C1_usere_narrow_fv",
		apiKey:           testdbs.USERE_API_KEY,
		fsName:           useraProject,
		fvName:           narrowFV,
		httpCode:         http.StatusOK,
		customersCols:    narrowFVCustomersCols,
		transactionsCols: narrowFVTransactionsCols,
	},
	// F2: consumer-created FV over exactly the granted columns
	// Hopsworks allowed creating it because the selection fit the grant;
	// serving it must fit the same boundary.
	{
		scenario:         "F2_usere_own_fv",
		apiKey:           testdbs.USERE_API_KEY,
		fsName:           "usere_project",
		fvName:           "usere_own_fv",
		httpCode:         http.StatusOK,
		customersCols:    narrowFVCustomersCols,
		transactionsCols: narrowFVTransactionsCols,
	},

	// D0: restricted member without grants sees NOTHING - the security-gap
	// acceptance test (before the fix userj got 200 via bare project_team
	// membership because team_role was never read)
	{
		scenario:       "D0_userj_full_fv",
		apiKey:         testdbs.USERJ_API_KEY,
		fsName:         useraProject,
		fvName:         fullFV,
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denyDB,
	},
	// The default-deny applies to every FV of the store.
	{
		scenario:       "D0_userj_narrow_fv",
		apiKey:         testdbs.USERJ_API_KEY,
		fsName:         useraProject,
		fvName:         narrowFV,
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denyDB,
	},

	// D2: restricted, customers granted entirely - FVs needing
	// transactions_fg still denied
	// The restricted grant ladder behaves exactly like the shared_* one:
	// a whole-FG grant does not unlock FVs joining an ungranted FG.
	{
		scenario:       "D2_userk_full_fv",
		apiKey:         testdbs.USERK_API_KEY,
		fsName:         useraProject,
		fvName:         fullFV,
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denyTxTable,
	},
	// Narrow FV needs num_transactions_30d - userk has no transactions
	// grant of any shape.
	{
		scenario:       "D2_userk_narrow_fv",
		apiKey:         testdbs.USERK_API_KEY,
		fsName:         useraProject,
		fvName:         narrowFV,
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denyTxTable,
	},
	// F4: restricted users create FVs in usera_project itself
	// userk owns no project, so his customers-only FV lives in usera's
	// store; serving it needs only his customers grant.
	{
		scenario:      "F4_userk_own_fv",
		apiKey:        testdbs.USERK_API_KEY,
		fsName:        useraProject,
		fvName:        "userk_own_fv",
		httpCode:      http.StatusOK,
		customersCols: narrowFVCustomersCols,
	},

	// D3: restricted, customers whole + transactions subset - the granted
	// boundary in both directions under one grant
	// Full FV crosses the subset boundary (total_spend_30d etc.) -> denial
	// names the ungranted columns.
	{
		scenario:       "D3_userl_full_fv",
		apiKey:         testdbs.USERL_API_KEY,
		fsName:         useraProject,
		fvName:         fullFV,
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denySpendCol,
	},
	// The narrow FV stays inside the granted subset -> serves.
	{
		scenario:         "D3_userl_narrow_fv",
		apiKey:           testdbs.USERL_API_KEY,
		fsName:           useraProject,
		fvName:           narrowFV,
		httpCode:         http.StatusOK,
		customersCols:    narrowFVCustomersCols,
		transactionsCols: narrowFVTransactionsCols,
	},
	// F5: userl's own FV (in usera's store, like userk's) built over
	// exactly his granted columns.
	{
		scenario:         "F5_userl_own_fv",
		apiKey:           testdbs.USERL_API_KEY,
		fsName:           useraProject,
		fvName:           "userl_own_fv",
		httpCode:         http.StatusOK,
		customersCols:    narrowFVCustomersCols,
		transactionsCols: narrowFVTransactionsCols,
	},

	// D5: userm is restricted in usera_project (customers entirely) AND
	// owner of his own empty userm_project - the extra membership must not
	// widen his access in usera_project (recording_D5.json)
	// Full FV blocked by the ungranted transactions table, like userk.
	{
		scenario:       "D5_userm_full_fv",
		apiKey:         testdbs.USERM_API_KEY,
		fsName:         useraProject,
		fvName:         fullFV,
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denyTxTable,
	},
	// Owning userm_project must not leak any transactions access.
	{
		scenario:       "D5_userm_narrow_fv",
		apiKey:         testdbs.USERM_API_KEY,
		fsName:         useraProject,
		fvName:         narrowFV,
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denyTxTable,
	},
	// D5d: another restricted user's FV is readable when its constituent
	// features fit the caller's grant - and not otherwise
	// userk's customers-only FV fits userm's customers grant: creator
	// identity is irrelevant, only the feature set counts.
	{
		scenario:      "D5d_userm_reads_userk_own_fv",
		apiKey:        testdbs.USERM_API_KEY,
		fsName:        useraProject,
		fvName:        "userk_own_fv",
		httpCode:      http.StatusOK,
		customersCols: narrowFVCustomersCols,
	},
	// userl's FV references num_transactions_30d, which userm lacks ->
	// denied although both are restricted members of the same store.
	{
		scenario:       "D5d_userm_reads_userl_own_fv",
		apiKey:         testdbs.USERM_API_KEY,
		fsName:         useraProject,
		fvName:         "userl_own_fv",
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denyTxTable,
	},

	// consumers must not reach each other's stores either
	// userb's store-entirely grant covers usera_project only - userc's
	// store (where userc_own_fv's metadata lives) stays off-limits.
	{
		scenario:       "userb_reads_userc_own_fv",
		apiKey:         testdbs.USERB_API_KEY,
		fsName:         "userc_project",
		fvName:         "userc_own_fv",
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denyUsercDB,
	},
	// An outsider is denied on a consumer's store just like on usera's.
	{
		scenario:       "userf_reads_usere_own_fv",
		apiKey:         testdbs.USERF_API_KEY,
		fsName:         "usere_project",
		fvName:         "usere_own_fv",
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denyUsereDB,
	},
}

func Test_Sharing_GetFeatureVector(t *testing.T) {
	for _, tt := range fvReadTests {
		t.Run(tt.scenario, func(t *testing.T) {
			customerID := 1
			fsReq := createFeatureStoreRequest(tt.fsName, tt.fvName, customerID)
			fsResp := getFeatureStoreResponseWithKey(t, tt.apiKey, fsReq,
				tt.errMsgContains, tt.httpCode)
			if tt.httpCode == http.StatusOK {
				validateVectorWithMySQL(t, fsResp,
					expectedVector(t, customerID, tt.customersCols, tt.transactionsCols))
			}
		})
	}
}

func Test_Sharing_GetBatchFeatureVector(t *testing.T) {
	for _, tt := range fvReadTests {
		t.Run(tt.scenario, func(t *testing.T) {
			customerIDs := []int{1, 2}
			batchReq := createBatchFeatureStoreRequest(tt.fsName, tt.fvName, customerIDs)
			batchResp := getBatchFeatureStoreResponseWithKey(t, tt.apiKey, batchReq,
				tt.errMsgContains, tt.httpCode)
			if tt.httpCode == http.StatusOK {
				expected := make([][]string, len(customerIDs))
				for i, customerID := range customerIDs {
					expected[i] = expectedVector(t, customerID,
						tt.customersCols, tt.transactionsCols)
				}
				validateBatchVectorsWithMySQL(t, batchResp, expected)
			}
		})
	}
}

type pkReadTest struct {
	scenario string
	apiKey   string
	table    string
	// nil means all columns (no readColumns in the request)
	readColumns    []string
	httpCode       int
	errMsgContains string
	// on 200: columns validated against mysqld
	respKVs []interface{}
}

// The generic-API matrix (E-rules): pk-read on the producer's online FG
// tables. This one table drives both the pk-read and batch pk-read tests.
var pkReadTests = []pkReadTest{
	// customers table: wholly granted to b (store), c/d/e (FG share),
	// k/l (restricted grant) - E1
	// userb's store-entirely share opens every table of the database.
	{
		scenario: "E1_userb_customers",
		apiKey:   testdbs.USERB_API_KEY,
		table:    customersTable,
		httpCode: http.StatusOK,
		respKVs:  []interface{}{"age", "country"},
	},
	// userc's whole-FG share grants the FG's table without a store share.
	{
		scenario: "E1_userc_customers",
		apiKey:   testdbs.USERC_API_KEY,
		table:    customersTable,
		httpCode: http.StatusOK,
		respKVs:  []interface{}{"age", "country"},
	},
	// userd's only grant is exactly this FG - the pk-read it covers works.
	{
		scenario: "E1_userd_customers",
		apiKey:   testdbs.USERD_API_KEY,
		table:    customersTable,
		httpCode: http.StatusOK,
		respKVs:  []interface{}{"age", "country"},
	},
	// usere holds customers whole (his column subset is on transactions).
	{
		scenario: "E1_usere_customers",
		apiKey:   testdbs.USERE_API_KEY,
		table:    customersTable,
		httpCode: http.StatusOK,
		respKVs:  []interface{}{"age", "country"},
	},
	// Restricted whole-FG grant behaves like a project-level FG share.
	{
		scenario: "E1_userk_customers",
		apiKey:   testdbs.USERK_API_KEY,
		table:    customersTable,
		httpCode: http.StatusOK,
		respKVs:  []interface{}{"age", "country"},
	},
	// userl's customers grant is entire, so a whole-row read is fine.
	{
		scenario: "E1_userl_customers",
		apiKey:   testdbs.USERL_API_KEY,
		table:    customersTable,
		httpCode: http.StatusOK,
		respKVs:  []interface{}{"age", "country"},
	},

	// E5: no grant at all (outsider / restricted without grants)
	// Outsider: no membership, no share - denied at the database level.
	{
		scenario:       "E5_userf_customers",
		apiKey:         testdbs.USERF_API_KEY,
		table:          customersTable,
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denyDB,
	},
	// Restricted member with zero grants: membership alone opens nothing.
	{
		scenario:       "D0_userj_customers",
		apiKey:         testdbs.USERJ_API_KEY,
		table:          customersTable,
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denyDB,
	},
	// The default-deny holds for every table of the store.
	{
		scenario:       "D0_userj_transactions",
		apiKey:         testdbs.USERJ_API_KEY,
		table:          transactionsTable,
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denyDB,
	},

	// transactions table, all columns: only store-entirely (b) and
	// whole-FG (c) grants qualify; d/k have no transactions grant (E2),
	// e/l hold only a column subset so an all-columns read is denied (E3)
	// Store-entirely: whole-row reads allowed on any table.
	{
		scenario: "A1_userb_transactions",
		apiKey:   testdbs.USERB_API_KEY,
		table:    transactionsTable,
		httpCode: http.StatusOK,
		respKVs:  []interface{}{"num_transactions_30d", "total_spend_30d"},
	},
	// userc's second whole-FG share covers this table entirely too.
	{
		scenario: "B3_userc_transactions",
		apiKey:   testdbs.USERC_API_KEY,
		table:    transactionsTable,
		httpCode: http.StatusOK,
		respKVs:  []interface{}{"num_transactions_30d", "total_spend_30d"},
	},
	// B4/E2: userd's placeholder store row + customers share grant nothing
	// here - the unshared table stays closed.
	{
		scenario:       "B4_userd_transactions",
		apiKey:         testdbs.USERD_API_KEY,
		table:          transactionsTable,
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denyTxTable,
	},
	// Restricted mirror of B4: customers grant does not leak to this table.
	{
		scenario:       "D2_userk_transactions",
		apiKey:         testdbs.USERK_API_KEY,
		table:          transactionsTable,
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denyTxTable,
	},
	// Column-subset grantee asking for the WHOLE row: denied, like
	// SELECT * on a column-granted MySQL table (1143).
	{
		scenario:       "E3_usere_transactions_all_cols",
		apiKey:         testdbs.USERE_API_KEY,
		table:          transactionsTable,
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denyTxTable,
	},
	// Same rule for the restricted column-subset grant.
	{
		scenario:       "E3_userl_transactions_all_cols",
		apiKey:         testdbs.USERL_API_KEY,
		table:          transactionsTable,
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denyTxTable,
	},

	// transactions table, granted column subset: readable for the
	// column-level grantees (E3), still denied without any grant
	// Projecting only the shared column succeeds for the shared_* grantee.
	{
		scenario:    "E3_usere_transactions_granted_cols",
		apiKey:      testdbs.USERE_API_KEY,
		table:       transactionsTable,
		readColumns: []string{"num_transactions_30d"},
		httpCode:    http.StatusOK,
		respKVs:     []interface{}{"num_transactions_30d"},
	},
	// ... and for the restricted_* grantee with the same subset.
	{
		scenario:    "E3_userl_transactions_granted_cols",
		apiKey:      testdbs.USERL_API_KEY,
		table:       transactionsTable,
		readColumns: []string{"num_transactions_30d"},
		httpCode:    http.StatusOK,
		respKVs:     []interface{}{"num_transactions_30d"},
	},
	// Narrowing the projection does not help userd - he has no grant of
	// any shape on this table.
	{
		scenario:       "B4_userd_transactions_granted_cols",
		apiKey:         testdbs.USERD_API_KEY,
		table:          transactionsTable,
		readColumns:    []string{"num_transactions_30d"},
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denyTxTable,
	},

	// D5: userm's restricted grant works like userk's despite his extra
	// own-project membership
	// His customers grant serves whole-row reads, exactly like userk's.
	{
		scenario: "D5_userm_customers",
		apiKey:   testdbs.USERM_API_KEY,
		table:    customersTable,
		httpCode: http.StatusOK,
		respKVs:  []interface{}{"age", "country"},
	},
	// Owning userm_project must not open usera's ungranted table.
	{
		scenario:       "D5_userm_transactions",
		apiKey:         testdbs.USERM_API_KEY,
		table:          transactionsTable,
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denyTxTable,
	},

	// transactions table, ungranted column: denial names the column
	// total_spend_30d is outside usere's subset - denied by name (the
	// MySQL 1143 mirror).
	{
		scenario:       "E3_usere_transactions_ungranted_col",
		apiKey:         testdbs.USERE_API_KEY,
		table:          transactionsTable,
		readColumns:    []string{"total_spend_30d"},
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denySpendCol,
	},
	// Same boundary for the restricted subset grant.
	{
		scenario:       "E3_userl_transactions_ungranted_col",
		apiKey:         testdbs.USERL_API_KEY,
		table:          transactionsTable,
		readColumns:    []string{"total_spend_30d"},
		httpCode:       http.StatusUnauthorized,
		errMsgContains: denySpendCol,
	},
	// Control: the same column is perfectly readable for the
	// store-entirely grantee - the denial above is about the grant, not
	// the column.
	{
		scenario:    "A1_userb_transactions_any_col",
		apiKey:      testdbs.USERB_API_KEY,
		table:       transactionsTable,
		readColumns: []string{"total_spend_30d"},
		httpCode:    http.StatusOK,
		respKVs:     []interface{}{"total_spend_30d"},
	},
}

func Test_Sharing_PKRead(t *testing.T) {
	for _, tt := range pkReadTests {
		t.Run(tt.scenario, func(t *testing.T) {
			testInfo := api.PKTestInfo{
				PkReq: api.PKReadBody{
					Filters:     testclient.NewFiltersKVs("customer_id", 1),
					ReadColumns: newReadColumns(tt.readColumns),
				},
				Table:          tt.table,
				Db:             useraProject,
				HttpCode:       tt.httpCode,
				ErrMsgContains: tt.errMsgContains,
				RespKVs:        tt.respKVs,
			}
			pkReadWithKey(t, tt.apiKey, testInfo)
		})
	}
}

func Test_Sharing_BatchPKRead(t *testing.T) {
	// A batch is authorized as a whole: one unauthorized sub-operation
	// rejects the entire request. Sub-responses of authorized batches are
	// validated against mysqld per operation.
	t.Run("E3_usere_mixed_batch_granted", func(t *testing.T) {
		// Whole-table op + granted-subset op in one batch: both serve.
		batchPKReadWithKey(t, testdbs.USERE_API_KEY, []batchPKReadOp{
			{db: useraProject, table: customersTable, customerID: 1,
				respKVs: []interface{}{"age", "country"}},
			{db: useraProject, table: transactionsTable, customerID: 2,
				readColumns: []string{"num_transactions_30d"},
				respKVs:     []interface{}{"num_transactions_30d"}},
		}, "", http.StatusOK)
	})
	t.Run("E3_usere_mixed_batch_ungranted_col", func(t *testing.T) {
		// One ungranted column in the second op rejects the whole batch.
		batchPKReadWithKey(t, testdbs.USERE_API_KEY, []batchPKReadOp{
			{db: useraProject, table: customersTable, customerID: 1},
			{db: useraProject, table: transactionsTable, customerID: 1,
				readColumns: []string{"total_spend_30d"}},
		}, denySpendCol, http.StatusUnauthorized)
	})
	t.Run("A1_userb_mixed_batch", func(t *testing.T) {
		// Store-entirely: whole-row ops on both tables in one batch.
		batchPKReadWithKey(t, testdbs.USERB_API_KEY, []batchPKReadOp{
			{db: useraProject, table: customersTable, customerID: 1,
				respKVs: []interface{}{"age", "country"}},
			{db: useraProject, table: transactionsTable, customerID: 2,
				respKVs: []interface{}{"num_transactions_30d", "total_spend_30d"}},
		}, "", http.StatusOK)
	})
	t.Run("D0_userj_batch", func(t *testing.T) {
		// Restricted member without grants: batch denied outright.
		batchPKReadWithKey(t, testdbs.USERJ_API_KEY, []batchPKReadOp{
			{db: useraProject, table: customersTable, customerID: 1},
		}, denyDB, http.StatusUnauthorized)
	})
	t.Run("B4_userd_batch_with_unshared_table", func(t *testing.T) {
		// The granted customers op cannot carry the unshared transactions
		// op - the whole batch is rejected naming the table.
		batchPKReadWithKey(t, testdbs.USERD_API_KEY, []batchPKReadOp{
			{db: useraProject, table: customersTable, customerID: 1},
			{db: useraProject, table: transactionsTable, customerID: 1},
		}, denyTxTable, http.StatusUnauthorized)
	})
}

// createFeatureStoreRequest builds a single-entry request for the given FV.
func createFeatureStoreRequest(fsName string, fvName string, customerID int) *api.FeatureStoreRequest {
	return feature_store.CreateFeatureStoreRequest(fsName, fvName, 1,
		[]string{"customer_id"},
		[]interface{}{[]byte(fmt.Sprintf("%d", customerID))},
		nil, nil)
}

func createBatchFeatureStoreRequest(fsName string, fvName string, customerIDs []int) *api.BatchFeatureStoreRequest {
	batchValues := make([][]interface{}, len(customerIDs))
	for i, id := range customerIDs {
		batchValues[i] = []interface{}{[]byte(fmt.Sprintf("%d", id))}
	}
	return batchfeaturestore.CreateBatchFeatureStoreRequest(fsName, fvName, 1,
		[]string{"customer_id"}, batchValues, nil, [][]interface{}{})
}
