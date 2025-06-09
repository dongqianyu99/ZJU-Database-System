#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"
using namespace std;
extern "C" {
  int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.
   **/
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
  
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

/**
 * 执行引擎的核心入口点。
 * 输入一条 SQL 语句后，
 * 解析器 (Parser) 会将这条语句转换成一个抽象语法树 (Abstract Syntax Tree, AST)，
 * 这个 ast 参数就是那棵树的根节点。
 * Execute 函数的职责就是接收这棵 AST，
 * 然后根据 AST 的类型来决定下一步该做什么，并最终执行相应的操作。
**/
dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;//没有有效的语法树
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);//创建执行上下文，比如当前的事务（虽然本次任务不要求实现事务）、以及最重要的——目录管理器 (CatalogManager)。目录管理器知道当前数据库中有哪些表、哪些索引等元数据信息。
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {//根据 ast 节点的类型 (ast->type_) 来判断这是一条什么类型的 SQL 命令。
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());//规划器的任务是将 AST 转换成一个物理查询计划 (一个由 PlanNode 组成的树)。
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  if (ast->type_ == kNodeSelect)
      delete planner.plan_->OutputSchema();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(("./databases/" + db_name).c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  if (db_name == current_db_)
    current_db_ = "";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
       << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * Create table
 */
//内部使用的临时结构，用于解析列信息
struct TempParsedColumnInfo {
    std::string name;
    TypeId type_id{TypeId::kTypeInvalid};
    uint32_t len_for_char = 0;
    bool is_unique_from_col_def = false; // 来自列级 UNIQUE
    bool is_not_null_from_col_def = false; // 来自列级 NOT NULL
};
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  //检查是否已选数据库
  if(current_db_.empty()){
    cout << "No database selected." << endl;
    return DB_FAILED;
  }
  //检查执行上下文和目录管理器是否有效
  if(!context || !context->GetCatalog()) {
    LOG(ERROR) << "Execution context or catalog manager is null for CreateTable." ;
    return DB_FAILED;
  }
  // AST 结构: kNodeCreateTable -> child_ (kNodeIdentifier: table_name) -> next_ (kNodeColumnList: 列定义列表)
   if (ast == nullptr || ast->type_ != kNodeCreateTable ||
          ast->child_ == nullptr || ast->child_->type_ != kNodeIdentifier || ast->child_->val_ == nullptr || // 表名节点
          ast->child_->next_ == nullptr || ast->child_->next_->type_ != kNodeColumnDefinitionList) { // 列定义列表节点
      LOG(ERROR) << "Syntax error: Invalid AST structure for CREATE TABLE statement." << std::endl;
      return DB_FAILED; // 早期返回：AST 结构错误
  }

  string table_name = ast->child_->val_;
  if (table_name.empty()) {
    LOG(ERROR) << "Syntax error: Table name cannot be empty." << std::endl;
    return DB_FAILED; // 早期返回：表名为空
  }
  TableInfo *existing_table_info = nullptr;
  if (context->GetCatalog()->GetTable(table_name, existing_table_info) == DB_SUCCESS) {
    LOG(ERROR) << "Table '" << table_name << "' already exists." << std::endl;
    return DB_TABLE_ALREADY_EXIST; // 早期返回：表已存在
  }
  vector<TempParsedColumnInfo> parsed_cols;
  vector<string> pk_cols;//主键列名
  set<string> pk_col_set; // 用于检查列名唯一性

  //遍历列定义
  pSyntaxNode current_list_item = ast->child_->next_->child_;
  vector<Column *> columns; //用于解析途中出错清理

  while (current_list_item != nullptr) {
    if(current_list_item->type_ == kNodeColumnDefinition) {
      //解析列定义
      TempParsedColumnInfo temp_col_info;
      if (current_list_item->child_ == nullptr || current_list_item->child_->type_ != kNodeIdentifier ||
          current_list_item->child_->val_ == nullptr) {
        LOG(ERROR) << "Syntax error: Column name cannot be empty." << std::endl;
        return DB_FAILED; // 早期返回：列名为空
      }

      temp_col_info.name = current_list_item->child_->val_;

      // Check "UNIQUE" here
      // TODO: Didn't handle "NOT NULL"
      if (current_list_item->val_ != nullptr) {
          string constraint_name = current_list_item->val_;
          transform(constraint_name.begin(), constraint_name.end(), constraint_name.begin(), ::toupper);// 转换为大写
          if (constraint_name == "UNIQUE")
            temp_col_info.is_unique_from_col_def = true; // 列级 UNIQUE
      }

      //获取列类型
      pSyntaxNode type_node = current_list_item->child_->next_;
      if (type_node == nullptr || type_node->type_ != kNodeColumnType) {
        LOG(ERROR) << "Syntax error: Column type is missing for column '" << temp_col_info.name << "'." << std::endl;
        return DB_FAILED; // 早期返回：列类型缺失
      }
      if (type_node->val_ == nullptr) {
        LOG(ERROR) << "Syntax error: Column type cannot be empty for column '" << temp_col_info.name << "'." << std::endl;
        return DB_FAILED; // 早期返回：列类型为空
      }
      string type_str = type_node->val_;
      if (type_str == "int") {
        temp_col_info.type_id = TypeId::kTypeInt;
      } else if (type_str == "float") {
        temp_col_info.type_id = TypeId::kTypeFloat;
      } else if (type_str == "char") {
        temp_col_info.type_id = TypeId::kTypeChar;
        pSyntaxNode len_node = type_node->child_;
        if (len_node == nullptr || len_node->type_ != kNodeNumber || len_node->val_ == nullptr) {
          LOG(ERROR) << "Syntax error: Length for char type is missing for column '" << temp_col_info.name << "'." << std::endl;
          for(auto &col : columns) {
            delete col; // 清理已解析的列
          }
          return DB_FAILED; // 早期返回：字符类型长度缺失
        }
        string len_str = len_node->val_;
        if(len_str.find('-')!=string::npos || len_str.find('.')!=string::npos) {
          LOG(ERROR) << "Syntax error: Invalid length for char type for column '" << temp_col_info.name << "'." << std::endl;
          for(auto &col : columns) {
            delete col; // 清理已解析的列
          }
          return DB_FAILED; // 早期返回：字符类型长度无效
        }
        try {
          unsigned long parsed_len = stoul(len_str);//转换为unsigned long
          if(parsed_len > std::numeric_limits<uint32_t>::max()) {
            LOG(ERROR) << "Syntax error: Length for char type exceeds maximum limit for column '" << temp_col_info.name << "'." << std::endl;
            for(auto &col : columns) {
              delete col; // 清理已解析的列
            }
            return DB_FAILED; // 早期返回：字符类型长度超出限制
          }
          temp_col_info.len_for_char = static_cast<uint32_t>(parsed_len);        
        }catch(const std::invalid_argument &e) {
          LOG(ERROR) << "Syntax error: Invalid length for char type for column '" << temp_col_info.name << "'. Error: " << e.what() << std::endl;
          for(auto &col : columns) {
            delete col; 
          }
          return DB_FAILED; 
        } 
      }
      else {
          LOG(ERROR) << "Unsupported column type '" << type_str << "' for column '" << temp_col_info.name << "'." << std::endl;
          for(auto &col : columns) {
            delete col; 
          }
          return DB_FAILED; 
      }
      //解析列级约束（UNIQUE和NOT NULL）
      pSyntaxNode constraint_node = type_node->next_;
      // pSyntaxNode constraint_node = current_list_item;
      while (constraint_node != nullptr) {
        // [WARNING] This might not work properly!
        if(constraint_node->type_ == kNodeIdentifier && constraint_node->val_ != nullptr) {
        // if(constraint_node->type_ == kNodeColumnDefinition && constraint_node->val_ != nullptr) {
          string constraint_name = constraint_node->val_;
          transform(constraint_name.begin(), constraint_name.end(), constraint_name.begin(), ::toupper);// 转换为大写
          if (constraint_name == "UNIQUE") {
            temp_col_info.is_unique_from_col_def = true; // 列级 UNIQUE
          } else if (constraint_name == "NOT") {
            if (constraint_node->next_ != nullptr &&
                constraint_node->next_->type_ == kNodeIdentifier &&
                constraint_node->next_->val_){
              string next_constraint_name = constraint_node->next_->val_;
              transform(next_constraint_name.begin(), next_constraint_name.end(), next_constraint_name.begin(), ::toupper);// 转换为大写
              if (next_constraint_name == "NULL") {
                temp_col_info.is_not_null_from_col_def = true; // 列级 NOT NULL
                constraint_node = constraint_node->next_->next_; // 跳过 "NULL" 
              } 
            }
          }
        }
        constraint_node = constraint_node->next_; // 移动到下一个约束节点
      }
      parsed_cols.push_back(temp_col_info); // 将解析的列信息添加到列表中
    }
    else if(current_list_item->type_ == kNodeColumnList) { //表级约束
      if (!pk_cols.empty()) {
        LOG(ERROR) << "Syntax error: Duplicate primary key definition." << std::endl;
        for(auto &col : columns) {
          delete col; // 清理已解析的列
        }
        return DB_FAILED; // 早期返回：主键定义重复
      }
      pSyntaxNode pk_node = current_list_item->child_;
      if(!pk_node) {
        LOG(ERROR) << "Syntax error: Primary key definition is missing." << std::endl;
        for(auto &col : columns) {
          delete col; // 清理已解析的列
        }
        return DB_FAILED; // 早期返回：主键定义缺失
      }
      set<string> pk_col_set; // 用于检查主键列名唯一性
      while(pk_node != nullptr) {
        if (!pk_node -> val_ || pk_node->type_ != kNodeIdentifier) {
          LOG(ERROR) << "Syntax error: Primary key column name cannot be empty." << std::endl;
          for(auto &col : columns) {
            delete col; // 清理已解析的列
          }
          return DB_FAILED; // 早期返回：主键列名为空
        }

        if (!pk_node->val_ )  return DB_FAILED; // 早期返回：主键列名为空
        string pk_name = pk_node->val_;
        pk_cols.push_back(pk_name);
        pk_col_set.insert(pk_name);
        pk_node = pk_node->next_; // 移动到下一个主键列节点
      }
    }
    else{
      LOG(ERROR) << "Syntax error: Invalid column definition in CREATE TABLE." << std::endl;
      for(auto &col : columns) {
        delete col; // 清理已解析的列
      }
      return DB_FAILED; //无效的列定义
    }
    current_list_item = current_list_item->next_; // 移动到下一个列定义节点
  }

  //创建Column对象
  vector<Column *> columns_to_create;
  columns_to_create.reserve(parsed_cols.size());//预分配空间以提高性能
  uint32_t col_id = 0;
  for(const auto& temp_col : parsed_cols) {
    if (temp_col.type_id == TypeId::kTypeInvalid) {
      LOG(ERROR) << "Syntax error: Invalid column type for column '" << temp_col.name << "'." << std::endl;
      for(auto &col : columns_to_create) {
        delete col; // 清理已解析的列
      }
      return DB_FAILED; // 早期返回：无效的列类型
    }

    bool is_pk = pk_col_set.count(temp_col.name) > 0;
    bool is_unique = temp_col.is_unique_from_col_def || is_pk; // 如果是主键，则视为唯一
    bool is_not_null = temp_col.is_not_null_from_col_def || is_pk; // 如果是主键，则视为非空
    Column *col = nullptr;
    if(temp_col.type_id == TypeId::kTypeChar) {
      col = new Column(temp_col.name, temp_col.type_id, temp_col.len_for_char, col_id++, ~is_not_null, is_unique);
    } else {
      col = new Column(temp_col.name, temp_col.type_id, col_id++, ~is_not_null, is_unique);
    }
    columns_to_create.push_back(col);
  }

  //创建Schema对象
  TableSchema *schema = new TableSchema(columns_to_create, true);
  //直接调用目录管理器的 CreateTable 方法来创建表
  Txn *txn = context->GetTransaction();
  TableInfo *table_info = nullptr;
  dberr_t ret = context->GetCatalog()->CreateTable(table_name, schema,txn, table_info);
  if (ret != DB_SUCCESS) {
    LOG(ERROR) << "Failed to create table: " << table_name << ". Error code: " << ret;
    for(auto &col : columns_to_create) {
      delete col; // 清理已创建的列
    }
    delete schema; // 清理Schema对象
    ExecuteInformation(ret); //输出错误信息
    return ret; //如果创建表失败，输出错误信息并返回错误码
  }
  delete schema; // 创建成功后，清理Schema对象
  schema = nullptr; // 避免悬空指针
  
  //创建主键索引
  bool result = true;
  if (!pk_cols.empty()) {
    // 创建主键索引
    vector<uint32_t> pk_col_ids;
    for (const auto &pk_col : pk_cols) {
      string pk_index_name = table_info->GetTableName() + "_PK"; 
      IndexInfo *pk_index = nullptr;
      ret = context->GetCatalog()->CreateIndex(
        table_info->GetTableName(), pk_index_name, pk_cols, txn, pk_index,"bptree");
      if (ret != DB_SUCCESS) {
        result = false;
        LOG(ERROR) << "Failed to create primary key index for table: " << table_name << ". Error code: " << ret;
        ExecuteInformation(ret); //输出错误信息
        dberr_t drop_ret = context->GetCatalog()->DropTable(table_info->GetTableName());
        if (drop_ret != DB_SUCCESS) {
          LOG(ERROR) << "Failed to drop table after index creation failure: " << table_name << ". Error code: " << drop_ret;
        }else {
          LOG(INFO) << "Table " << table_name << " dropped after index creation failure." << std::endl;
        }
        return ret; //如果创建主键索引失败，输出错误信息并返回错误码
      }
    }
    if (result) {
      for (const auto&  col : parsed_cols) {
        if (col.is_unique_from_col_def && pk_col_set.find(col.name) == pk_col_set.end()){
          string unique_index_name = table_info->GetTableName() + "_" + col.name + "_UNIQUE";
          vector<string> unique_col_names = {col.name};
          IndexInfo *unique_index = nullptr;
          ret = context->GetCatalog()->CreateIndex(
            table_info->GetTableName(), unique_index_name, unique_col_names, txn, unique_index,"bptree");
          if (ret != DB_SUCCESS) {
            // 对于非主键的唯一索引创建失败，通常只记录警告，不回滚整个表。
            LOG(WARNING) << "Table '" << table_name << "' created, but failed to create unique index for column '"
                          <<  col.name<< "' (Index: '" << unique_index_name << "'). Error: " << ret << std::endl;
          }
        }
      }
    }
  }
  cout << "Table " << table_name << " created." << endl;
  return DB_SUCCESS;
}
/**
 * DropTable
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
 if (current_db_.empty()) {
    cout << "No database selected." << endl;
    return DB_FAILED;
  }
  if (!context || !context->GetCatalog()) {
    LOG(ERROR) << "Execution context or catalog manager is null for DropTable.";
    return DB_FAILED;
  }
  // AST 结构: kNodeDropTable -> child_ (kNodeIdentifier: table_name)
  if (!ast || !ast->child_ || !ast->child_->val_) {
      LOG(ERROR) << "DB_SYNTAX_ERROR：Invalid AST for DROP TABLE.";
      return DB_FAILED;
  }
  string table_name = ast->child_->val_; //获取要删除的表名
  dberr_t ret = context->GetCatalog()->DropTable(table_name); //直接调用目录管理器的 DropTable 方法来删除表
  
  if (ret != DB_SUCCESS) {
    ExecuteInformation(ret); //如果删除失败，输出错误信息
    return ret;
  } 
  cout << "Table " << table_name << " dropped." << endl; 
  return ret;
}

/**
 * 
 * ShowIndexes - 逐表查找索引，如果找到，输出索引信息
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected." << endl;
    return DB_FAILED;
  }
  if (!context || !context->GetCatalog()) {
      LOG(ERROR) << "Execution context or catalog manager is null.";
      return DB_FAILED;
  }
  vector<TableInfo *> tables;
  dberr_t ret = context->GetCatalog()->GetTables(tables);
  if (ret != DB_SUCCESS) {
    if(ret == DB_TABLE_NOT_EXIST) {
      cout << "Empty set." << endl;
      return DB_SUCCESS; 
    }
    ExecuteInformation(ret); //如果获取表失败，输出错误信息
    return ret;
  }
  vector<vector<IndexInfo *>> all_indexes(tables.size());
  for(TableInfo *table : tables) {
    vector<IndexInfo *> indexes;
    ret = context->GetCatalog()->GetTableIndexes(table->GetTableName(), indexes);
    if (ret != DB_SUCCESS) {
      ExecuteInformation(ret); //如果获取索引失败，输出错误信息
      return ret;
    }
    all_indexes.push_back(indexes);
  }
  for(size_t i = 0; i < tables.size(); i++) {
    TableInfo *table = tables[i];
    vector<IndexInfo *> &indexes = all_indexes[i];
    if (indexes.empty()) {
      cout << "Table " << table->GetTableName() << " has no indexes." << endl;
      continue;
    }
    cout << "Indexes for table " << table->GetTableName() << ":" << endl;
    for(const auto& index : indexes){
      cout <<" -> " <<index->GetIndexName() << " on columns: ";
      bool first_column = true;
      for(const auto& column : index->GetIndexKeySchema()->GetColumns()){
        if (!first_column) {
            std::cout << " ";
        }
        cout << " [ " << column->GetName() << " ] ";
        first_column = false;
      }
      cout << endl;
    }
  }
}
/**
 * CreateIndex - 创建索引
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if (context == nullptr || current_db_.empty()) {
    std::cout << "No database selected." << std::endl;
    return DB_FAILED;
  }
  if (!context->GetCatalog()) {
    LOG(ERROR) << "Execution context or catalog manager is null for CreateIndex.";
    return DB_FAILED;
  }
  // AST: kNodeCreateIndex -> child (index_name) -> next (table_name) -> next (column_list)
  // column_list 节点 -> child (第一个列名) -> next (下一个列名) ...
  if (!ast || !ast->child_ || !ast->child_->val_ || // index_name
      !ast->child_->next_ || !ast->child_->next_->val_ || // table_name
      !ast->child_->next_->next_ || ast->child_->next_->next_->type_ != kNodeColumnList || // column_list node
      !ast->child_->next_->next_->child_ ) { // at least one column in list
      LOG(ERROR) << "SYNTAX ERROR:Invalid AST for CREATE INDEX.";
      return DB_FAILED;
  }
  string index_name = ast->child_->val_; //索引名
  pSyntaxNode table_name_node = ast->child_->next_; //表名节点
  string table_name = table_name_node->val_; //表名
  vector<string> column_names;
  pSyntaxNode column_list_node = table_name_node->next_; //列名列表节点
  pSyntaxNode column_node = column_list_node->child_; //列名列表
  TableInfo *table_info = nullptr;
  dberr_t ret = context->GetCatalog()->GetTable(table_name, table_info); //表信息
  if (ret != DB_SUCCESS) {
    ExecuteInformation(ret); //如果获取表失败，输出错误信息
    return ret;
  }
  // 遍历列名列表节点，收集所有列名
  for (; column_node != nullptr; column_node = column_node->next_) {
    if (column_node->type_ != kNodeIdentifier || !column_node->val_) {
      LOG(ERROR) << "SYNTAX ERROR:Invalid column name in CREATE INDEX.";
      return DB_FAILED;
    }
    column_names.push_back(column_node->val_); //列名
  }
  //处理索引类型
  string index_type = "bptree"; //默认索引类型为 bptree
  if (column_list_node->next_!= nullptr) {
    index_type = column_list_node->next_->child_->val_; //如果有指定索引类型，则使用指定的类型
  }
  IndexInfo *index_info; // 用于存储创建后的索引元信息

  ret = context->GetCatalog()->CreateIndex(table_name, index_name, column_names, context->GetTransaction(), index_info, index_type);//调用目录管理器的 CreateIndex 方法来创建索引
  if(ret != DB_SUCCESS){
    ExecuteInformation(ret);
    return ret;
  }
  // 获取表的堆存储对象 (TableHeap)，用于遍历表中的数据行
  TableHeap *table_heap = table_info->GetTableHeap();
  if (table_heap == nullptr) {
    LOG(ERROR) << "Table heap is null for table: " << table_name;
    return DB_FAILED;
  }
  // 遍历表中的每一行数据，为每一行创建索引条目并将其插入到实际的索引数据结构中-填充索引内容的关键步骤
  for(auto row = table_heap->Begin(context->GetTransaction()); row != table_heap->End(); ++row) {
    RowId rid = row->GetRowId();
    vector<Field> key_fields;
    // index_info->GetIndexKeySchema() 返回了索引包含哪些列以及这些列的顺序
    for (auto column : index_info->GetIndexKeySchema()->GetColumns()) {
      auto field = row->GetField(column->GetTableInd());
      key_fields.push_back(*field);//获取索引键字段
    }

    // 将索引键 (row_index) 和对应的 RowId (rid) 插入到实际的索引数据结构中
    // index_info->GetIndex() 返回底层的索引实现 (如B+树)
    Row row_index(key_fields);
    ret = index_info->GetIndex()->InsertEntry(row_index, rid, context->GetTransaction());//将索引条目插入到索引中
    if (ret != DB_SUCCESS) {
      ExecuteInformation(ret); //如果插入索引失败，输出错误信息
      return ret;
    } 
  }
  cout << "Index " << index_name << " created on table " << table_name << " for columns: ";
  for (const auto &col_name : column_names) {
    cout << col_name << " ";
  }
  cout << endl;
  return DB_SUCCESS;
}

/**
 * DropIndex - 逐表查找索引，如果找到，调用目录管理器的 DropIndex 方法删除索引
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected." << endl;
    return DB_FAILED;
  }
  if (!context || !context->GetCatalog()) {
    LOG(ERROR) << "Execution context or catalog manager is null for DropIndex.";
    return DB_FAILED;
  }
  // AST 结构: kNodeDropIndex -> child_ (kNodeIdentifier: index_name)
  if (!ast || !ast->child_ || ast->type_ != kNodeDropIndex || !ast->child_->val_) {
      LOG(ERROR) << "DB_SYNTAX_ERROR：Invalid AST for DROP INDEX.";
      return DB_FAILED;
  }

  vector<TableInfo *> tables;
  if (context->GetCatalog()->GetTables(tables) != DB_SUCCESS) {
    cout << "No tables found." << endl;
    return DB_FAILED;
  }
  string index_name = ast->child_->val_; //获取要删除的索引名
  string table_name;
  for (const auto &table : tables) {
    vector<IndexInfo *> indexes;
    if (context->GetCatalog()->GetTableIndexes(table->GetTableName(), indexes) == DB_SUCCESS) {
      for (const auto &index : indexes) {
        if (index->GetIndexName() == index_name) {
          table_name = table->GetTableName();
          break;
        }
      }
    }
    if (!table_name.empty()) break; // 找到对应的表名后退出循环
  }
  if (table_name.empty()) {
    cout << "Index " << index_name << " not found." << endl;
    return DB_INDEX_NOT_FOUND;
  }
  dberr_t ret = context->GetCatalog()->DropIndex(table_name, index_name); //直接调用目录管理器的 DropIndex 方法来删除索引
  if (ret != DB_SUCCESS) {
    ExecuteInformation(ret); //如果删除失败，输出错误信息
    return ret;
  }
  cout << "Index " << index_name << " on table " << table_name << " dropped." << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * Execfile - 读取指定的 SQL 脚本文件，逐行解析和执行其中的 SQL 语句。
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  // ast 中获取文件名并进行有效性检查
  if (!ast || !ast->child_ || !ast->child_->val_) {
    LOG(ERROR) << "SYNTAX ERROR:Invalid AST for EXECFILE.";
    return DB_FAILED;
  }
  string file_name(ast->child_->val_);
  //打开sql脚本文件
  ifstream sql_script_file(file_name);
  if (!sql_script_file.is_open()) {
    LOG(ERROR) << "Failed to open file: " << file_name;
    return DB_FAILED;
  }
  //存储当前读取的sql语句
  string sql_statement;
  dberr_t result = DB_SUCCESS; //跟踪执行结果
  int line_number = 0; //跟踪行号，以定位错误
  char ch;//当前读取字符
  //逐字符读取、构建语句、解析和执行
  while (sql_script_file.get(ch)) {
    sql_statement += ch; //累积字符到当前语句
    if (ch == '\n') {
      line_number++;
    }
    if (ch == ';') {
      sql_statement.erase(0,sql_statement.find_first_not_of(" \t\n\r\f\v")); //去除前导空白
      sql_statement.erase(sql_statement.find_last_not_of(" \t\n\r\f\v") + 1); //去除尾部空白
      // 跳过空语句或只包含分号的语句
      if (sql_statement.empty() || sql_statement == ";") {
        sql_statement.clear();
        continue;
      }
      MinisqlParserInit();
      //创建词法分析缓冲区并进行词法分析
      YY_BUFFER_STATE buffer = yy_scan_string(sql_statement.c_str());
      if (buffer == nullptr) {
        LOG(ERROR) << "Failed to create buffer for SQL statement: " << sql_statement;
        result = DB_FAILED;
        MinisqlParserFinish();
        break;
      }
      //调用Bison解析器进行语法分析
      int parse_result = yyparse();
      // yy_delete_buffer(buffer); //删除词法分析缓冲区

      auto result = Execute(MinisqlGetParserRootNode()); //获取解析结果
      MinisqlParserFinish(); 
      yy_delete_buffer(buffer); 
      yylex_destroy(); //清理解析器状态
      ExecuteInformation(result); //输出执行结果信息
      if(result == DB_QUIT){
        break;
      }
      sql_statement.clear(); //清空当前语句，准备下一条
    }
  }
  sql_script_file.close(); 

  return DB_SUCCESS; 
}

/**
 * Quit
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
 ExecuteInformation(DB_QUIT);
 return DB_QUIT;
}
