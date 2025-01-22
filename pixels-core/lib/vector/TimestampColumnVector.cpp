//
// Created by liyu on 12/23/23.
//

#include "vector/TimestampColumnVector.h"
#include<ctime>
#include<sstream>
#include<iomanip>

TimestampColumnVector::TimestampColumnVector(int precision, bool encoding): ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    TimestampColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, encoding);
}

TimestampColumnVector::TimestampColumnVector(uint64_t len, int precision, bool encoding): ColumnVector(len, encoding) {
    std::cout<<"encoding: "<<encoding<<std::endl;
    this->precision = precision;
    // if(encoding) {
    //     posix_memalign(reinterpret_cast<void **>(&this->times), 64,
    //                    len * sizeof(long));
    // } else {
    //     this->times = nullptr;
    // }
    posix_memalign(reinterpret_cast<void **>(&this->times), 64,
                       len * sizeof(long));
}


void TimestampColumnVector::close() {
    if(!closed) {
        ColumnVector::close();
        if(encoding && this->times != nullptr) {
            free(this->times);
        }
        this->times = nullptr;
    }
}

void TimestampColumnVector::print(int rowCount) {
    throw InvalidArgumentException("not support print longcolumnvector.");
//    for(int i = 0; i < rowCount; i++) {
//        std::cout<<longVector[i]<<std::endl;
//		std::cout<<intVector[i]<<std::endl;
//    }
}

TimestampColumnVector::~TimestampColumnVector() {
    if(!closed) {
        TimestampColumnVector::close();
    }
}

void * TimestampColumnVector::current() {
    if(this->times == nullptr) {
        return nullptr;
    } else {
        return this->times + readIndex;
    }
}

/**
     * Set a row from a value, which is the days from 1970-1-1 UTC.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param days
 */
void TimestampColumnVector::set(int elementNum, long ts) {
    if(elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    times[elementNum] = ts;
    // TODO: isNull
}

void TimestampColumnVector::add(long value) {
	if (writeIndex >= length) {
		ensureSize(writeIndex * 2, true);
	}
	int index = writeIndex++;
	set(index, value);
	isNull[index] = false;
}

// void DateColumnVector::add(Date value) {
// 	if (writeIndex >= length) {
// 		ensureSize(writeIndex * 2, true);
// 	}
// 	int index = writeIndex++;
// 	set(index, value.days);
// 	isNull[index] = false;
// }

constexpr int64_t MICROS_PER_SEC = 1000000;
constexpr int64_t NANOS_PER_MICROS = 1000;

int64_t stringTimestampToMicros(const std::string &timestamp) {
	std::tm tm = {};
	std::istringstream ss(timestamp);

	// 解析 YYYY-MM-DD HH:mm:ss 部分
	ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
	if (ss.fail()) {
		throw std::runtime_error("Invalid timestamp format: " + timestamp);
	}
	tm.tm_isdst = -1;
	int64_t seconds = timegm(&tm);
	int64_t nanos = 0;
	if (ss.peek() == '.') {
		ss.ignore();
		std::string nanosStr;
		ss >> nanosStr;
		nanosStr.append(9 - nanosStr.length(), '0');
		nanos = std::stoll(nanosStr);
	}
	return seconds * MICROS_PER_SEC + nanos / NANOS_PER_MICROS;
}

void TimestampColumnVector::add(std::string &value) {
	add(stringTimestampToMicros(value));
}

void TimestampColumnVector::ensureSize(uint64_t size, bool preserveData) {
	ColumnVector::ensureSize(size, preserveData);
	if (length < size) {
		long *oldVector = times;
		posix_memalign(reinterpret_cast<void **>(&times), 32, size * sizeof(long));
		if (preserveData) {
			std::copy(oldVector, oldVector + length, times);
		}
		delete[] oldVector;
		memoryUsage += (long)sizeof(long) * (size - length);
		resize(size);
	}
}