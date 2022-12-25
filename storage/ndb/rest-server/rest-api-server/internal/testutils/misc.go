package testutils

import "flag"

const HOPSWORKS_TEST_API_KEY = "bkYjEz6OTZyevbqt.ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub"

var WithRonDB = flag.Bool("with-rondb", false, "test with a running RonDB instance")
