#ifndef LOG_HPP
#define LOG_HPP

#include <string>

class LogConfig {
public:
	std::string level;
	std::string filePath;
	int maxSizeMb;
	int maxBackups;
	int maxAge;
	LogConfig();
};

#endif