#include "log.hpp"

LogConfig::LogConfig() {
	this->level     = "warn";
	this->filePath  = "";
	this->maxSizeMb = 100;
	this->maxBackups = 10;
	this->maxAge    = 30;
}
