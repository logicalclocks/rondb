/*
 * Copyright (C) 2024 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

#include "../src/api_key.hpp"
#include "../src/config_structs.hpp"
#include "connection.hpp"
#include "resources/embeddings.hpp"
#include "rdrs_dal.h"
#include "rdrs_hopsworks_dal.h"
#include "rdrs_rondb_connection.hpp"
#include "rdrs_rondb_connection_pool.hpp"

#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <chrono>
#include <queue>
#include <mutex>
#include <condition_variable>

template <typename T>
class SafeQueue {
private:
    std::queue<T> queue_;
    std::mutex mutex_;
    std::condition_variable cond_;

public:
    void push(T value) {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push(std::move(value));
        cond_.notify_one();
    }

    T pop() {
        std::unique_lock<std::mutex> lock(mutex_);
        cond_.wait(lock, [this]{ return !queue_.empty(); });
        T value = std::move(queue_.front());
        queue_.pop();
        return value;
    }
};

extern RDRSRonDBConnectionPool *rdrsRonDBConnectionPool;
const std::string HOPSWORKS_TEST_API_KEY = "bkYjEz6OTZyevbqt.ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub";

class APIKeyTest : public ::testing::Test {
protected:
	static void SetUpTestSuite() {
		RS_Status status = RonDBConnection::init_rondb_connection(globalConfigs.ronDB, globalConfigs.ronDbMetaDataCluster);
		if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
			errno = status.http_code;
			exit(errno);
		}
	}

	static void TearDownTestSuite() {
		RS_Status status = RonDBConnection::shutdown_rondb_connection();
		if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
			errno = status.http_code;
			exit(errno);
		}
	}
};

void test_key(const std::shared_ptr<Cache>& cache) {
	const std::string existentDB = DB001;
	const std::string existentDB2 = DB002;
	const std::string fakeDB = "test3";

	std::string apiKey = "bkYjEz6OTZyevbqT.ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub";
	RS_Status status = cache->validate_api_key(apiKey, {existentDB});
	EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) << "Wrong prefix was falsely validated";

	apiKey = "bkYjEz6OTZyevbqT";
	status = cache->validate_api_key(apiKey, {});
	EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) << "Missing secret was falsely validated";

	apiKey = "bkYjEz6OTZyevbq.ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub";
	status = cache->validate_api_key(apiKey, {});
	EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) << "Wrong length prefix was falsely validated";

	// correct api key but wrong db. this api key can not access test3 db
	status = cache->validate_api_key(HOPSWORKS_TEST_API_KEY, {fakeDB});
	EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) << "Inexistent database was falsely validated";

	// correct api key
	status = cache->validate_api_key(HOPSWORKS_TEST_API_KEY, {existentDB});
	EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) << "No error expected; error: " << status.message;

	// no errors
	status = cache->validate_api_key(HOPSWORKS_TEST_API_KEY, {existentDB, existentDB2});
	EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) << "No error expected; error: " << status.message;
}

TEST_F(APIKeyTest, TestAPIKey1) {
	AllConfigs conf = AllConfigs::get_all();
	if (!conf.security.apiKey.useHopsworksAPIKeys) {
		std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
	}

	std::vector<std::shared_ptr<Cache>> caches;
	std::shared_ptr<Cache> cache = std::make_shared<Cache>();
	caches.push_back(std::move(cache));

	for (auto &cache : caches) {
		test_key(cache);
	}
}

void test_update_cache_every_n_seconds(const std::shared_ptr<Cache>& cache, const AllConfigs& conf) {
	std::string apiKey = HOPSWORKS_TEST_API_KEY;
	auto databases = {DB001, DB002};

	RS_Status status = cache->validate_api_key(apiKey, databases);
	EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) << "No error expected; error: " << status.message;

	auto lastUpdated1 = cache->last_updated(apiKey);
	// waiting 2 * cacheRefreshIntervalMS to ensure the update trigger has run
	std::this_thread::sleep_for(std::chrono::milliseconds(2 * conf.security.apiKey.cacheRefreshIntervalMS + 
		conf.security.apiKey.cacheRefreshIntervalJitterMS));
	auto lastUpdated2 = cache->last_updated(apiKey);
	EXPECT_NE(lastUpdated1, lastUpdated2) << "Cache entry was not updated";

	status = cache->validate_api_key(apiKey, databases);
	EXPECT_EQ(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) << "No error expected; error: " << status.message;
}

// Check that cache is updated every N secs
TEST_F(APIKeyTest, TestAPIKeyCache1) {	
	AllConfigs conf = AllConfigs::get_all();
	if (!conf.security.apiKey.useHopsworksAPIKeys) {
		std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
	}

	// To speed up the tests
	conf.security.apiKey.cacheRefreshIntervalMS = 1000;
	conf.security.apiKey.cacheRefreshIntervalJitterMS = 100;
	conf.security.apiKey.cacheUnusedEntriesEvictionMS = conf.security.apiKey.cacheRefreshIntervalMS * 2;
	AllConfigs::set_all(conf);

	std::vector<std::shared_ptr<Cache>> caches;
	std::shared_ptr<Cache> cache = std::make_shared<Cache>();
	caches.push_back(std::move(cache));

	for (auto &cache : caches) {
		test_update_cache_every_n_seconds(cache, conf);
	}
}

void test_update_cache_every_n_seconds_unauthorized(const std::shared_ptr<Cache>& cache, const AllConfigs& conf) {
	std::string apiKey = HOPSWORKS_TEST_API_KEY;
	const std::string db3 = "test3";

	RS_Status status = cache->validate_api_key(apiKey, {db3});
	EXPECT_NE(status.http_code, static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) << "Database should not exist. Expected test to fail";

	auto lastUpdated1 = cache->last_updated(apiKey);
	// waiting 2 * cacheRefreshIntervalMS to ensure the update trigger has run
	std::this_thread::sleep_for(std::chrono::milliseconds(2 * conf.security.apiKey.cacheRefreshIntervalMS + 
		conf.security.apiKey.cacheRefreshIntervalJitterMS));
	auto lastUpdated2 = cache->last_updated(apiKey);
	EXPECT_NE(lastUpdated1, lastUpdated2) << "Cache entry was not updated";
}

// check that cache is updated every N secs even if the user is not authorized to access a DB
TEST_F(APIKeyTest, TestAPIKeyCache2) {	
	AllConfigs conf = AllConfigs::get_all();
	if (!conf.security.apiKey.useHopsworksAPIKeys) {
		std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
	}

	// To speed up the tests
	conf.security.apiKey.cacheRefreshIntervalMS = 1000;
	conf.security.apiKey.cacheRefreshIntervalJitterMS = 100;
	conf.security.apiKey.cacheUnusedEntriesEvictionMS = conf.security.apiKey.cacheRefreshIntervalMS * 2;
	AllConfigs::set_all(conf);

	std::vector<std::shared_ptr<Cache>> caches;
	std::shared_ptr<Cache> cache = std::make_shared<Cache>();
	caches.push_back(std::move(cache));

	for (auto &cache : caches) {
		test_update_cache_every_n_seconds_unauthorized(cache, conf);
	}
}

void test_load(const std::shared_ptr<Cache>& cache, const AllConfigs& conf) {
	SafeQueue<bool> ch;
	int numOps = 150;

	std::vector<std::thread> producers;
	producers.reserve(numOps);
	for (int i = 0; i < numOps; ++i) {
		producers.push_back(std::thread([&ch, &cache] {
			unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
			std::mt19937 generator(seed);
	    std::uniform_int_distribution<int> distribution(0, HopsworksAPIKey_ADDITIONAL_KEYS - 1);
			int apiKeyNum = distribution(generator);
			std::ostringstream oss;
    	oss << std::setw(16) << std::setfill('0') << apiKeyNum << "." << HopsworksAPIKey_SECRET;
    	std::string apiKey = oss.str();

			auto DB = DB001;

			RS_Status status = cache->validate_api_key(apiKey, {DB});
			if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
				std::cout << "validation failed for api key: " << apiKey << "; error: " << status.message << std::endl;
				ch.push(false);
			} else {
				ch.push(true);
			}
		}));
	}

	for (auto& producer : producers) {
		producer.join();
	}

	bool pass = true;
	int failCount = 0;
	for (int i = 0; i < numOps; ++i) {
		bool val = ch.pop();
		if (!val) {
			pass = false;
			failCount++;
		}
	}

	if (!pass) {
		EXPECT_EQ(failCount, 0) << failCount << " key validations failed" << std::endl;
	}

	// wait for eviction time to pass
	std::this_thread::sleep_for(std::chrono::milliseconds(2 * conf.security.apiKey.cacheRefreshIntervalMS + 
		conf.security.apiKey.cacheUnusedEntriesEvictionMS +
		conf.security.apiKey.cacheRefreshIntervalJitterMS));

	// The content of the cache as a string
	std::string cacheContent = cache->to_string();
	EXPECT_EQ(cache->size(), 0) << "Cache was not cleared. Expected 0. Got " << cache->size() << " entries in the cache: " << cacheContent << std::endl;
}

// Test load. Generate lots of api key requests
TEST_F(APIKeyTest, TestAPIKeyCache3) {	
	AllConfigs conf = AllConfigs::get_all();
	if (!conf.security.apiKey.useHopsworksAPIKeys) {
		std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
	}

	// To speed up the tests
	conf.security.apiKey.cacheRefreshIntervalMS = 100;
	conf.security.apiKey.cacheRefreshIntervalJitterMS = 100;
	conf.security.apiKey.cacheUnusedEntriesEvictionMS = conf.security.apiKey.cacheRefreshIntervalMS * 2;
	AllConfigs::set_all(conf);

	std::vector<std::shared_ptr<Cache>> caches;
	std::shared_ptr<Cache> cache = std::make_shared<Cache>();
	caches.push_back(std::move(cache));

	for (auto &cache : caches) {
		test_load(cache, conf);
	}
}

void test_load_with_bad_keys(const std::shared_ptr<Cache>& cache, const AllConfigs& conf) {
	std::vector<std::string> badKeys = {
		"fvoHJCjkpof4WezF.4eed386ceb310e9976932cb279de2dab70c24a1ceb396e99dd29df3a1348f42e",
		"vNizYZEsK7Ip1AEt.3eb997b041460f59b90094bd7d07b30e385c1c72961a440b74f9dccf5dd467b2",
		"ASLMR5E2fZj99Urc.4beb809a7191cfb3f301467ead5cf9be537b42f3535d0b0a3262a2de14f4972c",
		"gvTjnkN9sT4f8QKP.0cbc2086d518d57f676c1876e98d01e2672a373cc7e6f46358759bb81cf70f34",
		"uIZsxwh0iS0ChVO7.5ae7a415290ade873b52c8002bfb52d9dce4d5e5e4a78e9d4413208171348917",
		"uLlYekGkIb9yRfMh.9cd7beab8983364db2cf78c68ba253026e782eac4795f4d76520236097ea30df",
		"8OHWKsjFL2ek4I1a.41a50648077779db9e106d7218d5fa67b9fda35ce08d3aa3a661a24fcf0c9499",
		"ha3Vwv76A00xXRbZ.e77ec23d18045492deefcf5566b75170a9ecf5c928c86a4891f9964a73cab537",
		"pws6Pt15Kb0Y6z1l.b34c14ba29320e8b3018abe7fa3af4c6b112c2c842e0734ed8a2f5ef5cd6881d",
		"tukhMkclDr5RZdhI.04f736fe234d93f4e9082c11ebdea670830c473e1e4a4e9cd8d97d59719fffd7",
		"zPZaJdxZ1SmzoXHE.827f25efe88e8d11f471c45ceedaee60c3b221fa92e26af02b8da98c8d767a54",
		"ImpGCVclSGHU7ENR.357a422b918617611847bc30a8c5663e0353860f5904931ea947e25718aa8997",
		"84q6WR9bxFYsuEin.202baf9ab9fca52404b69d00de9e7f6f30c1c04ef7d85b2258fa2c73799fe808",
		"EejFn4BA7M1QgGbF.1865aeeb01ce392444291d0a0ffe00b73e61b50b8f22be0ac94a7b991c1a5d7d",
		"CK8zsPaQYmtw5ZsL.a23f9207b5f525c0a2abe941eb716e98d6b061ccb3e44347bb9ce950fdbf3c89",
		"6ILEK6QkvLcztbEy.74f73637ae78e00136028b1254bf11669ebfe8fa26d47527daaf7bc4d0645d7a",
		"4yIbzqCZkugp5R9w.4166d6cd74a81f63e1da70c2f7dd7504d575d112be5a152bff4656d2d82189a3",
		"fIt8oIE8rIaHYLYD.e25466322dabc65f20a40623997a9a46c0fdebee77b9106021c8c0bf1dd29799",
		"iSHXZKLzVu0tYmtD.2ae442c50f6d650ea57d2f6d622416520751a8a53420344d57765a16b9d62436",
		"ArExPZKJfLAJA4y1.7953b49f0a777064f6f5cc3c0671777716b50355228bb18b4d8b9a1fae7ac185",
		"pGgsePMZcnylvYE1.ec0cf03ff4adc48ece53eb2648d7c3af47f9d9758207420ef825d2b5c22daf48",
		"dUoyC2RkLVhXBhKy.93b4b371637571ca90199640c64de38d3a25a4439e6bd535b1ba72b74665c24e",
		"eueiglqaGyTYTnxR.be28d684c25e076ec8dba8fe4bfa90c4e47b92413dd96ad72e58ed3386f5b561",
		"14KAZwpg2WEBd495.863ebf824a434d8be02cf0b271f311519553e822029c3629a7c2ada725c19b28",
		"rexNdYKxC5tnSL4l.e9b3b6916afbe43197957dace3d27e7a46e54885a0528751558e3bc551afc1ba",
		"O1HZWxhysrY9Gyzm.aa69b2998f98978a67a381ae1676d34d1f7840755e690751fe89c8f3bfa675a0",
		"P7TPSTmZqvrcjr0i.1a39245511be48308032a53febd2c87aa1ab36f9d12035a7da823ab2cff686f4",
		"imvrGrF8xXcv56zQ.4d32e9a1c5c9fe63cb90aa633ff6a296ec5b247aba6146f2fefcfe3edd052022",
		"YQxxA7asFLjDY9x0.0a23d2a1a51df84c949e1ee6ae0ea1fb3318fd09672e416c935450b230cf17ef",
		"hA7AaiWER7p28qMq.643bf6192b31f8b265f6994d5164cc1b2ff8e2c7cc8fd36b002895e2b5e0e7bf",
		"OnIM4JFEfh1Tc6ch.1333f69c2dfd4ef135fe57e138ca5900728145023d3e5eb86b9730f6f4e8ca94",
		"OIjBkc4nQzd5SuHz.b48344e10b505f9305c6a61189595ec2e627a207b5440321c9911ca52cbe3d16",
		"uoiI97cfErZxeXw8.eb7c6aecf770dd535dff036ef72320bf579d4ea606152ad6ab2716b2134f971c",
		"BcA1y9pD57bqnHPd.f230a76e8f8f77b4671e544dac5eb43944d9e6e5f56bfeafe775715715f57b4b",
		"qPmErPtUzAzBdOT9.38d95dbfc76735c17f955f964e92194fc124c9788ed7cedcb99ab1389ceac92c",
		"G9WcEVdVBtzqEynP.b21fefa7b3432afcacfae759378d43448819e9164d79e4722d206f791c9663e8",
		"O3RE8z7yfzzvkLwo.f3551462e8527e32cdb315039927765d93f19d9408ce781316746af99ce134c0",
		"UtZSTaFo6IK5gIuC.e9fda4f5b56b28f6c4deecb8c774db18571f577e0864cc994e4ce00ffe17a7c7",
		"xdCROBN8T5d7t8dr.9846f9940299c7a2703e37a12932c88590f3ca89ad029d85b9b59916c2eb902c",
		"anpA9GY4kpafqhdO.d90892a2d8c016b6ffc8601b1ba33fdd13b48d6fdd6ca7803146ccb330cc44d6",
		"vENzHsp96BhTKxtF.048866d815f193d81e7d5c3bdd4ed538eb9593f119785886400ddb78201a205b",
		"hLBB0nDBpimZLcQN.76ce9177012e0295396c1cb8c15c7c2b91113b463bfbe7c7675de7912458558f",
		"YLlLUBDyUDopgPY3.458e9b6247234511b3ab325448c7ab34ab9ea2e629f9408fa9eed163f2a1d639",
		"0xUzLhYchZHHTMll.ad9855a6717dfd87c34527772d39074592b5207727bf2b07a98b2c28755eecb5",
		"ovPMBJunjS51C963.b974ce48886da6cf54c6302da39c13d709e190edcdd65700b8eb48cd5183a488",
		"pd8cxgroWc4T1Vd6.af85e2aa4fa580f8d8b99c255124a7ec1c7c42be4cbdb9c16468cd879843992c",
		"dxnLAe5cNkp48Vyt.1c2b1bb75b0f1a24207b6b1de635e308b65e05fb52eb77f21c6b9d9d2c5aba6b",
		"GtyBzOVXEGBRqlGf.1339a00fbf24c7ce677d6b40bf1009594c2a0b682af3f542ce71bd6fb84e779b",
		"63b9DnA2A6R9CRV7.48f7bb59fb0db3fdc6d2818b4ade575c1cb8813b9033bd977eca7225dc188bd0",
		"0WhhpThaapAuVp2Y.5960341683854056c4eeae2e602398d0439c1b5062e0708695f0ce381dcd2f06",
		"CCoCcTeCQeanHKAR.fcb734f7856079bc5253f039ca1e94b8027a419c5a57ae032a45a40c2c911ff9",
		"s1GtULCFzsEmFkWQ.e0214b01fe437b0b0eff5884dd76099f6e1b68dfa6584b2d5ea3cc376bf02204",
		"r2QT2DgVFv8ko5Ol.e8dd5fbcf1d0cb4a0b882fd106a204d4462adc00e242d000021f659fa7cbd87b",
		"iMZkMSMHQGkvtoGe.67ab1785c3b3d7059f1362b0b4a3a5446b2bb2d069ad146eee6f4d1a65020b08",
		"QEHHKBVmYaKNZOGK.79d6f65520e05119cfb698ce256348796217188071ef6f0e56c0fc8f75bae3c3",
		"mRog42LN7DWoXF8v.4240ff3c6844cb59a50070fc8baa14358a9820a3d142ba962180ced2856b9894",
		"IFTU5sCXXgUXT8TM.f5504e74563805978804a507eb55a6e111e83abe8f7897a64ac84918fd7185a1",
		"geWmjrtRW2PwvHnX.46387e49e624828aab1f70004f4e161da62c2fbcb65353681b2b9d6891b5e7cd",
		"P9aQKTyQ4MEqgpT7.67c1bfaaae9c36e681bd509c3f5605e3c8adb680a6e6a2f216e6b14eabc4f599",
		"WDxm9Cc5j74Rx2Q1.858edb638c1f4295f1b62353dd2c875b330b40e5b9dedbf0489dc76482c50beb",
	};

	SafeQueue<bool> ch;
	int numOps = 1000;

	std::vector<std::thread> producers;
	producers.reserve(numOps);
	for (int i = 0; i < numOps; ++i) {
		producers.push_back(std::thread([&ch, &cache, &badKeys] {
			unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
			std::mt19937 generator(seed);
			std::string apiKey = badKeys[generator() % badKeys.size()];			

			auto DB = DB001;

			RS_Status status = cache->validate_api_key(apiKey, {DB});
			if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
				ch.push(true);
			} else {
				// TODO log
				ch.push(false);
			}
		}));
	}

	for (auto& producer : producers) {
		producer.join();
	}

	bool pass = true;
	int failCount = 0;
	for (int i = 0; i < numOps; ++i) {
		bool val = ch.pop();
		if (!val) {
			pass = false;
			failCount++;
		}
	}

	if (!pass) {
		EXPECT_EQ(failCount, 0) << failCount << " key validations failed" << std::endl;
	}

	// wait for eviction time to pass
	std::this_thread::sleep_for(std::chrono::milliseconds(2 * conf.security.apiKey.cacheRefreshIntervalMS + 
		conf.security.apiKey.cacheUnusedEntriesEvictionMS +
		conf.security.apiKey.cacheRefreshIntervalJitterMS));

	EXPECT_EQ(cache->size(), 0) << "Cache was not cleared. Expected 0. Got " << cache->size() << " entries in the cache" << std::endl;
}

// Test load. Generate lots of bad request for API Keys
TEST_F(APIKeyTest, TestAPIKeyCache4) {
	AllConfigs conf = AllConfigs::get_all();
	if (!conf.security.apiKey.useHopsworksAPIKeys) {
		std::cout << "tests may fail because Hopsworks API keys are deactivated" << std::endl;
	}

	// To speed up the tests
	conf.security.apiKey.cacheRefreshIntervalMS = 1000;
	conf.security.apiKey.cacheRefreshIntervalJitterMS = 100;
	conf.security.apiKey.cacheUnusedEntriesEvictionMS = conf.security.apiKey.cacheRefreshIntervalJitterMS * 2;
	AllConfigs::set_all(conf);

	std::vector<std::shared_ptr<Cache>> caches;
	std::shared_ptr<Cache> cache = std::make_shared<Cache>();
	caches.push_back(std::move(cache));

	for (auto &cache : caches) {
		test_load_with_bad_keys(cache, conf);
	}
}
