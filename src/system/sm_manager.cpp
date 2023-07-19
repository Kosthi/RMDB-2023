/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    //为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
        throw UnixError();
    }
    //创建系统目录
    DbMeta *new_db = new DbMeta();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db;  // 注意：此处重载了操作符<<

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    std::string cmd = "rm -r " + db_name;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string& db_name) {

    // 数据库不存在
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }

    // 数据已经打开
    if (!db_.name_.empty()) {
        throw DatabaseExistsError(db_name);
    }

    // 将当前工作目录设置为数据库目录
    if (chdir(db_name.c_str()) < 0) {
        throw UnixError();
    }

    // 从 DB_META_NAME 文件中加载数据库元数据
    std::ifstream ifs(DB_META_NAME);
    if (!ifs) {
        throw UnixError();
    }
    // 元数据读入内存
    ifs >> db_;

    // 打开数据中每个表的记录文件并读入
    for (auto& tab : db_.tabs_) {
        const std::string& tab_name = tab.first;
        auto& tab_meta = tab.second;
        fhs_[tab_name] = rm_manager_->open_file(tab_name);
        // 打开表上的所有索引并读入
        for (auto& index : tab_meta.indexes) {
            const std::string index_name = ix_manager_->get_index_name(tab_name, index.cols);
            ihs_[index_name] = ix_manager_->open_index(tab_name, index.cols);
        }
    }
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() {

    if (db_.name_.empty()) {
        throw DatabaseNotFoundError("db not open");
    }

    // 把数据库相关的元数据刷入磁盘中
    flush_meta();
    db_.name_.clear();
    db_.tabs_.clear();

    // 把每个表的数据文件刷入磁盘中
    for (auto& entry : fhs_) {
        rm_manager_->close_file(entry.second.get());
    }
    // 把每个表的索引文件刷入磁盘中
    for (auto& entry : ihs_) {
        ix_manager_->close_index(entry.second.get());
    }
    fhs_.clear();
    ihs_.clear();

    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context 
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

/**
 * @description: 显示表的索引数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context
 */
void SmManager::show_index(const std::string& tab_name, Context* context) {
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);

    RecordPrinter printer(3);

    std::vector<std::string> ss;
    std::string tmp;
    for (auto& index : db_.tabs_[tab_name].indexes) {
        ss.clear();
        tmp.clear();
        ss = {tab_name, "unique"};
        tmp += '(' + index.cols[0].name;
        outfile << "| " << tab_name << " | unique | (" << index.cols[0].name;
        for (int i = 1; i < index.cols.size(); ++i) {
            tmp += ',' + index.cols[i].name;
            outfile << "," << index.cols[i].name;
        }
        tmp += ')';
        outfile << ") |\n";
        ss.emplace_back(tmp);
        printer.print_index(ss, context);
    }
    outfile.close();
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context 
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context 
 */
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {
    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto &col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    // Create & open record file
    int record_size = curr_offset;  // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    flush_meta();
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {

    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }

    // 先获取表元数据
    TabMeta& tab = db_.get_table(tab_name);

    // 先关闭再删除表的数据文件
    rm_manager_->close_file(fhs_[tab_name].get());
    rm_manager_->destroy_file(tab_name);

    // 先关闭再删除表中对应的所有索引页面和文件
    for (auto& index : tab.indexes) {
        const std::string& index_name = ix_manager_->get_index_name(tab_name, index.cols);
        auto ih = std::move(ihs_.at(index_name));
        ix_manager_->close_index(ih.get());
        ix_manager_->destroy_index(ih.get(), tab_name, index.cols);
        ihs_.erase(index_name);
    }
    // 从数据库元数据中移除表信息
    db_.tabs_.erase(tab_name);
    fhs_.erase(tab_name);

    // 更新元数据
    flush_meta();
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    // 先获取表元数据
    TabMeta& tab = db_.get_table(tab_name);
    if (tab.is_index(col_names)) {
        throw IndexExistsError(tab_name, col_names);
    }
    std::vector<ColMeta> cols;
    int tot_col_len = 0;
    for (auto& col_name : col_names) {
        cols.emplace_back(*tab.get_col(col_name));
        tot_col_len += cols.back().len;
    }

    // 进行唯一性检查
    // 用不了哈希，试插法
    auto fh = fhs_[tab_name].get();
    ix_manager_->create_index(tab_name, cols);
    auto ih = ix_manager_->open_index(tab_name, cols);
    int idx = -1;
    for (RmScan rmScan(fh); !rmScan.is_end(); rmScan.next()) {
        auto rec = fh->get_record(rmScan.rid(), context);
        char* insert_data = new char[tot_col_len + 4];
        memcpy(insert_data + tot_col_len, &idx, 4);
        int offset = 0;
        for (auto& col : cols) {
            memcpy(insert_data + offset, rec->data + col.offset, col.len);
            offset += col.len;
        }
        std::vector<Rid> rid;
        if (ih->get_value(insert_data, &rid, context->txn_)) {
            ix_manager_->close_index(ih.get());
            ix_manager_->destroy_index(ih.get(), tab_name, col_names);
            throw InternalError("不满足唯一性约束！");
        }
        assert(ih->insert_entry(insert_data, rmScan.rid(), context->txn_) > 0);
    }

    auto index_name = ix_manager_->get_index_name(tab_name, col_names);
    if (ihs_.count(index_name)) {
        throw IndexExistsError(tab_name, col_names);
    }
    ihs_.emplace(index_name, std::move(ih));
    IndexMeta indexMeta = {tab_name, tot_col_len, static_cast<int>(cols.size()), cols};
    tab.indexes.emplace_back(indexMeta);

    // 更新元数据
    flush_meta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    // 获得表元数据
    TabMeta& tab = db_.get_table(tab_name);
    if (!tab.is_index(col_names)) {
        throw IndexNotFoundError(tab_name, col_names);
    }
    auto index_name = ix_manager_->get_index_name(tab_name, col_names);
    if (ihs_.count(index_name) == 0) {
        throw IndexNotFoundError(tab_name, col_names);
    }

    auto ih = std::move(ihs_.at(index_name));
    // 先关闭再清除索引文件
    ix_manager_->close_index(ih.get());
    ix_manager_->destroy_index(ih.get(), tab_name, col_names);
    // 清除 ihs 中的索引
    ihs_.erase(index_name);
    // 清除表元数据中的索引
    auto index_meta = tab.get_index_meta(col_names);
    tab.indexes.erase(index_meta);

    // 更新元数据
    flush_meta();
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& cols, Context* context) {
    // 获得表元数据
    TabMeta& tab = db_.get_table(tab_name);
    std::vector<std::string> cols_name;
    for (auto& col : cols) {
        cols_name.emplace_back(col.name);
    }
    auto index_meta = tab.get_index_meta(cols_name);
    auto index_name = ix_manager_->get_index_name(tab_name, cols);
    if (ihs_.count(index_name) == 0) {
        throw IndexNotFoundError(tab_name, cols_name);
    }

    auto ih = std::move(ihs_.at(index_name));
    // 先关闭再清除索引文件
    ix_manager_->close_index(ih.get());
    ix_manager_->destroy_index(ih.get(), tab_name, cols);
    // 清除 ihs 中的索引
    ihs_.erase(index_name);
    // 清除表元数据中的索引
    tab.indexes.erase(index_meta);

    // 更新元数据
    flush_meta();
}
