//
// Created by Shiping Yao on 2023/3/12.
//

#include <exception.h>
#include <string>

namespace yedis {
std::string Exception::ExceptionTypeToString(ExceptionType type) {
  switch (type) {
    case ExceptionType::INVALID:
      return "Invalid";
    case ExceptionType::OUT_OF_RANGE:
      return "Out of Range";
    case ExceptionType::CONVERSION:
      return "Conversion";
    case ExceptionType::UNKNOWN_TYPE:
      return "Unknown Type";
    case ExceptionType::DECIMAL:
      return "Decimal";
    case ExceptionType::MISMATCH_TYPE:
      return "Mismatch Type";
    case ExceptionType::DIVIDE_BY_ZERO:
      return "Divide by Zero";
    case ExceptionType::OBJECT_SIZE:
      return "Object Size";
    case ExceptionType::INVALID_TYPE:
      return "Invalid type";
    case ExceptionType::SERIALIZATION:
      return "Serialization";
    case ExceptionType::TRANSACTION:
      return "TransactionContext";
    case ExceptionType::NOT_IMPLEMENTED:
      return "Not implemented";
    case ExceptionType::EXPRESSION:
      return "Expression";
    case ExceptionType::CATALOG:
      return "Catalog";
    case ExceptionType::PARSER:
      return "Parser";
    case ExceptionType::BINDER:
      return "Binder";
    case ExceptionType::PLANNER:
      return "Planner";
    case ExceptionType::SCHEDULER:
      return "Scheduler";
    case ExceptionType::EXECUTOR:
      return "Executor";
    case ExceptionType::CONSTRAINT:
      return "Constraint";
    case ExceptionType::INDEX:
      return "Index";
    case ExceptionType::STAT:
      return "Stat";
    case ExceptionType::CONNECTION:
      return "Connection";
    case ExceptionType::SYNTAX:
      return "Syntax";
    case ExceptionType::SETTINGS:
      return "Settings";
    case ExceptionType::OPTIMIZER:
      return "Optimizer";
    case ExceptionType::NULL_POINTER:
      return "NullPointer";
    case ExceptionType::IO:
      return "IO";
    case ExceptionType::INTERRUPT:
      return "INTERRUPT";
    case ExceptionType::FATAL:
      return "FATAL";
    case ExceptionType::INTERNAL:
      return "INTERNAL";
    case ExceptionType::INVALID_INPUT:
      return "Invalid Input";
    case ExceptionType::OUT_OF_MEMORY:
      return "Out of Memory";
    case ExceptionType::PERMISSION:
      return "Permission";
    case ExceptionType::PARAMETER_NOT_RESOLVED:
      return "Parameter Not Resolved";
    case ExceptionType::PARAMETER_NOT_ALLOWED:
      return "Parameter Not Allowed";
    case ExceptionType::DEPENDENCY:
      return "Dependency";
    default:
      return "Unknown";
  }
}

Exception::Exception(const string &msg) : std::exception(), type(ExceptionType::INVALID), raw_message_(msg) {
  exception_message_ = msg;
}

Exception::Exception(ExceptionType exception_type, const string &message)
    : std::exception(), type(exception_type), raw_message_(message) {
  exception_message_ = ExceptionTypeToString(exception_type) + " Error: " + message;
}

const char *Exception::what() const noexcept {
  return exception_message_.c_str();
}

const std::string &Exception::RawMessage() const {
  return raw_message_;
}

std::string Exception::GetStackTrace(int max_depth) {
#ifdef YEDIS_DEBUG_STACKTRACE
  std::string result;
	auto callstack = std::unique_ptr<void *[]>(new void *[max_depth]);
	int frames = backtrace(callstack.get(), max_depth);
	char **strs = backtrace_symbols(callstack.get(), frames);
	for (int i = 0; i < frames; i++) {
		result += strs[i];
		result += "\n";
	}
	free(strs);
	return "\n" + result;
#else
  // Stack trace not available. Toggle DUCKDB_DEBUG_STACKTRACE in exception.cpp to enable stack traces.
  return "";
#endif
}

}
