/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <iostream>
#include <map>

// 此处重载了<<操作符，在ColMeta中进行了调用
template<typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::ostream &operator<<(std::ostream &os, const T &enum_val) {
    os << static_cast<int>(enum_val);
    return os;
}

template<typename T, typename = typename std::enable_if<std::is_enum<T>::value, T>::type>
std::istream &operator>>(std::istream &is, T &enum_val) {
    int int_val;
    is >> int_val;
    enum_val = static_cast<T>(int_val);
    return is;
}

struct Rid {
    int page_no;
    int slot_no;

    friend bool operator==(const Rid &x, const Rid &y) {
        return x.page_no == y.page_no && x.slot_no == y.slot_no;
    }

    friend bool operator!=(const Rid &x, const Rid &y) { return !(x == y); }
};

enum ColType {
    TYPE_INT, TYPE_FLOAT, TYPE_STRING, TYPE_BIGINT, TYPE_DATETIME
};

inline std::string coltype2str(ColType type) {
    std::map<ColType, std::string> m = {
            {TYPE_INT,    "INT"},
            {TYPE_FLOAT,  "FLOAT"},
            {TYPE_STRING, "STRING"},
            {TYPE_BIGINT, "BIGINT"},
            {TYPE_DATETIME, "DATETIME"}
    };
    return m.at(type);
}

class RecScan {
public:
    virtual ~RecScan() = default;

    virtual void next() = 0;

    virtual bool is_end() const = 0;

    virtual Rid rid() const = 0;
};

class DateTime {
public:
    DateTime() = default;

    DateTime(const uint16_t year_, const uint8_t month_, const uint8_t day_,
             const uint8_t hour_, const uint8_t minutes_, const uint8_t seconds_)
            : m_year(year_),
              m_month(month_),
              m_day(day_),
              m_hour(hour_),
              m_minutes(minutes_),
              m_seconds(seconds_) {
      m_valid = is_valid();
    }

    bool is_valid() const {
      uint8_t leap = 0;
      uint8_t months[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
      if (m_year % 400 == 0 || (m_year % 4 == 0 && m_year % 100)) leap = 1;
      if (m_month == 2 && m_day > months[1] + leap) return false;
      if (m_month != 2 && m_day > months[m_month - 1]) return false;
      return true;
    }

    bool valid() const { return m_valid; }

    std::string to_string() const {
      if (!valid()) return "";

      std::string result;
      result.reserve(19); // YYYY-MM-DD HH:MM:SS

      result.append(std::to_string(year())).append("-");
      result.append(month() < 10 ? "0" : "").append(std::to_string(static_cast<int>(month()))).append("-");
      result.append(day() < 10 ? "0" : "").append(std::to_string(static_cast<int>(day()))).append(" ");
      result.append(hour() < 10 ? "0" : "").append(std::to_string(static_cast<int>(hour()))).append(":");
      result.append(minutes() < 10 ? "0" : "").append(std::to_string(static_cast<int>(minutes()))).append(":");
      result.append(seconds() < 10 ? "0" : "").append(std::to_string(static_cast<int>(seconds())));

      return result;
    }

    friend std::ostream &operator<<(std::ostream &os, const DateTime &dateTime) {
      os << dateTime.to_string();
      return os;
    }

    int operator==(const DateTime &dateTime) const {
      std::string dt1 = this->to_string();
      std::string dt2 = dateTime.to_string();
      return (dt1 > dt2) ? 1 : (dt1 < dt2 ? -1 : 0);
    }

    uint16_t year() const { return m_year; }

    uint8_t month() const { return m_month; }

    uint8_t day() const { return m_day; }

    uint8_t hour() const { return m_hour; }

    uint8_t minutes() const { return m_minutes; }

    uint8_t seconds() const { return m_seconds; }

private:
    uint16_t m_year;
    uint8_t m_month;
    uint8_t m_day;

    uint8_t m_hour;
    uint8_t m_minutes;
    uint8_t m_seconds;
    bool m_valid;
};
