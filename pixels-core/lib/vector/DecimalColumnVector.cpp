//
// Created by yuly on 05.04.23.
//

#include "vector/DecimalColumnVector.h"

#include "duckdb/common/types/decimal.hpp"

/**
 * The decimal column vector with precision and scale.
 * The values of this column vector are the unscaled integer value
 * of the decimal. For example, the unscaled value of 3.14, which is
 * of the type decimal(3,2), is 314. While the precision and scale
 * of this decimal are 3 and 2, respectively.
 *
 * <p><b>Note: it only supports short decimals with max precision
 * and scale 18.</b></p>
 *
 * Created at: 05/03/2022
 * Author: hank
 */

DecimalColumnVector::DecimalColumnVector(int precision, int scale, bool encoding)
    : ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
	DecimalColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, scale, encoding);
}

DecimalColumnVector::DecimalColumnVector(uint64_t len, int precision, int scale, bool encoding)
    : ColumnVector(len, encoding) {
	// decimal column vector has no encoding so we don't allocate memory to
	// this->vector
	this->vector = nullptr;
	this->precision = precision;
	this->scale = scale;
	std::cout << "precision: " << precision << std::endl;
	using duckdb::Decimal;
	if (precision <= Decimal::MAX_WIDTH_INT16) {
		physical_type_ = PhysicalType::INT16;
		posix_memalign(reinterpret_cast<void **>(&vector), 32, len * sizeof(int16_t));
		memoryUsage += (uint64_t)sizeof(int16_t) * len;
	} else if (precision <= Decimal::MAX_WIDTH_INT32) {
		physical_type_ = PhysicalType::INT32;
		posix_memalign(reinterpret_cast<void **>(&vector), 32, len * sizeof(int32_t));
		memoryUsage += (uint64_t)sizeof(int32_t) * len;
	} else if (precision <= Decimal::MAX_WIDTH_INT64) {
		physical_type_ = PhysicalType::INT64;
		posix_memalign(reinterpret_cast<void **>(&vector), 32, len * sizeof(int64_t));
		memoryUsage += (uint64_t)sizeof(uint64_t) * len;
	}
	// } else if (precision <= Decimal::MAX_WIDTH_INT128) {
	// 	physical_type_ = PhysicalType::INT128;
	// 	// posix_memalign(reinterpret_cast<void **>(&vector), 32, len * sizeof(int128_t));
	// 	memoryUsage += (uint64_t)sizeof(uint64_t) * len;
	// }
	else {
		throw std::runtime_error("Decimal precision is bigger than the maximum supported width");
	}
}

void DecimalColumnVector::close() {
	if (!closed) {
		ColumnVector::close();
		if (physical_type_ == PhysicalType::INT16 || physical_type_ == PhysicalType::INT32) {
			free(vector);
		}
		vector = nullptr;
	}
}

void DecimalColumnVector::print(int rowCount) {
	//    throw InvalidArgumentException("not support print Decimalcolumnvector.");
	for (int i = 0; i < rowCount; i++) {
		std::cout << vector[i] << std::endl;
	}
}

DecimalColumnVector::~DecimalColumnVector() {
	if (!closed) {
		DecimalColumnVector::close();
	}
}

void *DecimalColumnVector::current() {
	if (vector == nullptr) {
		return nullptr;
	} else {
		return vector + readIndex;
	}
}

int DecimalColumnVector::getPrecision() {
	return precision;
}

int DecimalColumnVector::getScale() {
	return scale;
}

#include <cmath>
#include <utility>

// 计算数字的总位数（不包括小数点）
int calculatePrecision(long value) {
	if (value == 0)
		return 1;
	value = std::abs(value); // 处理负数
	return std::floor(std::log10(value)) + 1;
}

// 将浮点数value按指定精度scale进行四舍五入，返回整数值和总位数
std::pair<long, int> setDecimalScale(double value, int scale) {
	// 计算缩放因子
	long scaleFactor = std::pow(10, scale);

	// 将浮点数转换为放大后的数值并四舍五入
	long scaledValue = std::round(value * scaleFactor);

	// 计算总位数（precision）
	int precision = calculatePrecision(scaledValue);

	// 返回放大后的整数值和总位数
	return std::make_pair(scaledValue, precision);
}

void DecimalColumnVector::add(double value) {
	if (writeIndex >= length) {
		ensureSize(writeIndex * 2, true);
	}
	int index = writeIndex++;
	std::pair<long, int> result = setDecimalScale(value, scale);
	vector[index] = result.first;
	if (result.second > precision) {
		throw std::invalid_argument("value exceeds the allowed precision");
	}
	isNull[index] = false;
}

void DecimalColumnVector::add(std::string &value) {
	add(std::stod(value));
}

void DecimalColumnVector::add(float value) {
	add(static_cast<double>(value));
}

void DecimalColumnVector::ensureSize(uint64_t size, bool preserveData) {
	ColumnVector::ensureSize(size, preserveData);
	if (length < size) {
		long *oldVector = vector;
		posix_memalign(reinterpret_cast<void **>(&vector), 32, size * sizeof(long));
		if (preserveData) {
			std::copy(oldVector, oldVector + length, vector);
		}
		delete[] oldVector;
		memoryUsage += (long)sizeof(long) * (size - length);
		resize(size);
	}
}