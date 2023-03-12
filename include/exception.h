//
// Created by Shiping Yao on 2023/3/12.
//

#ifndef YEDIS_EXCEPTION_H
#define YEDIS_EXCEPTION_H

#include <stdexcept>
#include <string>
#include <vector>

// referenced from duckdb exception.hpp
//===--------------------------------------------------------------------===//
// Exception Types
//===--------------------------------------------------------------------===//
namespace yedis {

enum class ExceptionType {
  INVALID = 0,          // invalid type
  OUT_OF_RANGE = 1,     // value out of range error
  CONVERSION = 2,       // conversion/casting error
  UNKNOWN_TYPE = 3,     // unknown type
  DECIMAL = 4,          // decimal related
  MISMATCH_TYPE = 5,    // type mismatch
  DIVIDE_BY_ZERO = 6,   // divide by 0
  OBJECT_SIZE = 7,      // object size exceeded
  INVALID_TYPE = 8,     // incompatible for operation
  SERIALIZATION = 9,    // serialization
  TRANSACTION = 10,     // transaction management
  NOT_IMPLEMENTED = 11, // method not implemented
  EXPRESSION = 12,      // expression parsing
  CATALOG = 13,         // catalog related
  PARSER = 14,          // parser related
  PLANNER = 15,         // planner related
  SCHEDULER = 16,       // scheduler related
  EXECUTOR = 17,        // executor related
  CONSTRAINT = 18,      // constraint related
  INDEX = 19,           // index related
  STAT = 20,            // stat related
  CONNECTION = 21,      // connection related
  SYNTAX = 22,          // syntax related
  SETTINGS = 23,        // settings related
  BINDER = 24,          // binder related
  NETWORK = 25,         // network related
  OPTIMIZER = 26,       // optimizer related
  NULL_POINTER = 27,    // nullptr exception
  IO = 28,              // IO exception
  INTERRUPT = 29,       // interrupt
  FATAL = 30,           // Fatal exceptions are non-recoverable, and render the entire DB in an unusable state
  INTERNAL = 31,        // Internal exceptions indicate something went wrong internally (i.e. bug in the code base)
  INVALID_INPUT = 32,   // Input or arguments error
  OUT_OF_MEMORY = 33,   // out of memory
  PERMISSION = 34,      // insufficient permissions
  PARAMETER_NOT_RESOLVED = 35, // parameter types could not be resolved
  PARAMETER_NOT_ALLOWED = 36,  // parameter types not allowed
  DEPENDENCY = 37              // dependency
};

class Exception : public std::exception {
private:
  using string = std::string;

public:
  explicit Exception(const string &msg);
  Exception(ExceptionType exception_type, const string &message);

  ExceptionType type;

public:
  const char *what() const noexcept override;
  const string &RawMessage() const;

  static string ExceptionTypeToString(ExceptionType type);


private:
  string exception_message_;
  string raw_message_;
};


class IOException: public Exception {
public:
  IOException(const std::string &msg) : Exception(ExceptionType::IO, msg) {
  }
};
}
#endif //YEDIS_EXCEPTION_H
