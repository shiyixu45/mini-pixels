//
// Created by yuly on 06.04.23.
//

#include "vector/DateColumnVector.h"

DateColumnVector::DateColumnVector(uint64_t len, bool encoding): ColumnVector(len, encoding) {
    std::cout<<"encoding: "<<encoding<<std::endl;
	// if(encoding) {
    //     posix_memalign(reinterpret_cast<void **>(&dates), 32,
    //                    len * sizeof(int32_t));
	// } else {
	// 	this->dates = nullptr;
	// }
	posix_memalign(reinterpret_cast<void **>(&dates), 32,
                       len * sizeof(int32_t));
	memoryUsage += (long) sizeof(int) * len;
}

void DateColumnVector::close() {
	if(!closed) {
		if(encoding && dates != nullptr) {
			free(dates);
		}
		dates = nullptr;
		ColumnVector::close();
	}
}

void DateColumnVector::print(int rowCount) {
	for(int i = 0; i < rowCount; i++) {
		std::cout<<dates[i]<<std::endl;
	}
}

DateColumnVector::~DateColumnVector() {
	if(!closed) {
		DateColumnVector::close();
	}
}

/**
     * Set a row from a value, which is the days from 1970-1-1 UTC.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param days
 */
void DateColumnVector::set(int elementNum, int days) {
	if(elementNum >= writeIndex) {
		writeIndex = elementNum + 1;
	}
	dates[elementNum] = days;
	// TODO: isNull
}

void * DateColumnVector::current() {
    if(dates == nullptr) {
        return nullptr;
    } else {
        return dates + readIndex;
    }
}

void DateColumnVector::add(int value) {
	if (writeIndex >= length) {
		ensureSize(writeIndex * 2, true);
	}
	int index = writeIndex++;
	set(index, value);
	isNull[index] = false;
}

int dateToDays(std::string &dateStr) {
	if (dateStr.length() != 10 || dateStr[4] != '-' || dateStr[7] != '-') {
		throw std::invalid_argument("Invalid date format. Expected: yyyy-mm-dd");
	}
	try {
		int year = std::stoi(dateStr.substr(0, 4));
		int month = std::stoi(dateStr.substr(5, 2));
		int day = std::stoi(dateStr.substr(8, 2));
		struct tm timeinfo = {};
		timeinfo.tm_year = year - 1900;
		timeinfo.tm_mon = month - 1;
		timeinfo.tm_mday = day;
		timeinfo.tm_hour = 0;
		timeinfo.tm_min = 0;
		timeinfo.tm_sec = 0;
		timeinfo.tm_isdst = 0;
		time_t timestamp = timegm(&timeinfo);
		if (timestamp == -1) {
			throw std::runtime_error("Invalid date");
		}
		return timestamp / (24 * 60 * 60);
	} catch (const std::exception &e) {
		throw std::invalid_argument("Date parsing failed");
	}
}

void DateColumnVector::add(std::string &value) {
	add(dateToDays(value));
}

void DateColumnVector::ensureSize(uint64_t size, bool preserveData) {
	ColumnVector::ensureSize(size, preserveData);
	if (length < size) {
		int *oldVector = dates;
		posix_memalign(reinterpret_cast<void **>(&dates), 32, size * sizeof(int));
		if (preserveData) {
			std::copy(oldVector, oldVector + length, dates);
		}
		delete[] oldVector;
		memoryUsage += (long)sizeof(int) * (size - length);
		resize(size);
	}
}