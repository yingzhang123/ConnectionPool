#pragma once
#include <iostream>

#define LOG(str) \
	cout << __FILE__ << ":" << __LINE__ << " " << __TIMESTAMP__ << " : " << str << endl;