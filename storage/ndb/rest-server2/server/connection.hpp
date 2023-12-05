#ifndef CONNECTION_HPP
#define CONNECTION_HPP

#include <string>
#include "config_structs.hpp"
#include <exception>
#include <iostream>

class RonDBConnection {
public:
	RonDBConnection(RonDB& data_cluster, RonDB& meta_data_cluster) {
		try
		{
			init_rondb_connection(data_cluster, meta_data_cluster);
		}
		catch(const std::exception& e)
		{
			std::cerr << e.what() << '\n';
		}
		std::cout << "RonDB Connection Initialized" << std::endl;
	}

	~RonDBConnection() {
		try
		{
			shutdown_rondb_connection();
		}
		catch(const std::exception& e)
		{
			std::cerr << e.what() << '\n';
		}
		std::cout << "RonDB Connection Shutdown" << std::endl;
	}
private:
	void init_rondb_connection(RonDB& rondb_data_cluster, RonDB& rondb_meta_data_cluster);

	void shutdown_rondb_connection();
	
	void rondb_reconnect();
};

#endif // CONNECTION_HPP