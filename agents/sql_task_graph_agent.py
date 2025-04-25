# agents/sql_task_graph_agent.py

from typing import Dict, Any, List
import json
from utils.snowflake_utils import get_snowflake_connection

AGENT_STATE_HINT = {
    "requires": ["staging_tables", "final_table"],
    "produces": ["task_graph", "task_sqls", "execution_result"]
}

class SQLTaskGraphAgent:
    def __init__(self, input_mode="streamlit", warehouse="COMPUTE_WH", schedule="1 hour", 
                 task_prefix="ETL_", final_task_prefix="FINAL_"):
        self.input_mode = input_mode
        self.warehouse = warehouse
        self.schedule = schedule
        self.task_prefix = task_prefix
        self.final_task_prefix = final_task_prefix
        self.task_configs = []
        self.task_sqls = []
    
    def __call__(self, state: Dict[str, Any]) -> Dict[str, Any]:
        print("\n🔵 [SQLTaskGraphAgent] - Creating Snowflake Task Graph")
        
        # 🧠 Extract state inputs
        staging_tables = state.get("staging_tables", [])
        final_table = state.get("final_table")
        warehouse = state.get("warehouse", self.warehouse)
        schedule = state.get("schedule", self.schedule)
        
        # Process the tables and create task graph
        self.warehouse = warehouse
        self.schedule = schedule
        
        # First create all the task configurations and SQL statements
        self.process_cte_extractions(staging_tables, final_table)
        task_graph = self.get_task_graph_visualization()
        
        # Generate task structure summary for LLM output
        root_tasks = [task for task in self.task_configs if not task['dependencies']]
        dependent_tasks = [task for task in self.task_configs if task['dependencies']]
        
        # Execute the tasks using the Snowflake connection from utils
        try:
            snowflake_connection = get_snowflake_connection()
            executor = TaskGraphExecutor(snowflake_connection)
            execution_result = executor.execute_task_graph(self.task_sqls)
            print(f"✅ Task graph created successfully with {len(self.task_sqls)} tasks")
            
            if "chatbot_messages" in state:
                # Create a more detailed task summary
                task_summary = f"📊 Task Graph Summary:\n"
                task_summary += f"- Root Tasks ({len(root_tasks)}): {', '.join([t['name'] for t in root_tasks])}\n"
                task_summary += f"- Dependent Tasks ({len(dependent_tasks)}): {', '.join([t['name'] for t in dependent_tasks])}\n"
                
                # Add execution details
                task_summary += f"\n✅ Successfully created and activated {len(self.task_configs)} Snowflake tasks!"
                task_summary += f"\n- Scheduling: '{schedule}'"
                task_summary += f"\n- Warehouse: {warehouse}"
                
                # Add final task info if exists
                final_tasks = [task for task in self.task_configs if task.get('is_final', False)]
                if final_tasks:
                    task_summary += f"\n\n🏁 Final Task: {final_tasks[0]['name']}"
                    
                state["chatbot_messages"].append({
                    "sender": "assistant",
                    "text": task_summary
                })
                
        except Exception as e:
            execution_result = {"status": "error", "message": f"Error creating task graph: {str(e)}"}
            print(f"❌ Task graph creation error: {e}")
            
            if "chatbot_messages" in state:
                state["chatbot_messages"].append({
                    "sender": "assistant",
                    "text": f"❌ Error while creating task graph: `{str(e)}`"
                })
        
        # Update state with task graph information
        updated_state = {
            **state,
            "task_sqls": self.task_sqls,
            "task_graph": task_graph,
            "execution_result": execution_result,
            "current_step": "sql_task_graph"
        }
        
        # 🔽 Save JSON here - include workflow status if available
        if "workflow_status" in state:
            updated_state["workflow_status"] = state["workflow_status"]
            
        with open("task_graph_state.json", "w") as f:
            json.dump(updated_state, f, indent=4, default=str)
        
        return updated_state
    
    def process_cte_extractions(self, staging_tables, final_table=None):
        """Main method to process CTE extractions and create task graph"""
        self._create_task_configs(staging_tables, final_table)
        self._generate_task_creation_sql()
        return self.task_sqls
    
    def _create_task_configs(self, staging_tables, final_table=None):
        """Create task configurations from CTE extractions"""
        self.task_configs = []
        
        # Process staging tables
        for extract in staging_tables:
            table_name = extract['table_name'].split('.')[-1]
            task_name = f"{self.task_prefix}{table_name}"
            
            config = {
                "name": task_name,
                "sql": extract['create_statement'],
                "dependencies": [
                    f"{self.task_prefix}{dep.split('.')[-1]}" 
                    for dep in extract.get('depends_on', [])
                ],
                "is_final": False
            }
            self.task_configs.append(config)
        
        # Process final table if provided
        if final_table:
            table_name = final_table['table_name'].split('.')[-1]
            task_name = f"{self.final_task_prefix}{table_name}"
            
            dependencies = []
            for dep in final_table.get('depends_on', []):
                # Extract the table name part (last part after the dot)
                dep_name = dep.split('.')[-1]
                dependencies.append(f"{self.task_prefix}{dep_name}")
            
            config = {
                "name": task_name,
                "sql": final_table['create_statement'],
                "dependencies": dependencies,
                "is_final": True
            }
            self.task_configs.append(config)
    
    def _generate_task_creation_sql(self):
        """Generate Snowflake task creation SQL statements"""
        self.task_sqls = []
        
        # Create root tasks first
        root_tasks = [task for task in self.task_configs if not task['dependencies']]
        for task in root_tasks:
            sql = self._create_root_task_sql(task)
            self.task_sqls.append(sql)
        
        # Create dependent tasks
        dependent_tasks = [task for task in self.task_configs if task['dependencies']]
        for task in dependent_tasks:
            sql = self._create_dependent_task_sql(task)
            self.task_sqls.append(sql)
        
        # Add task resumption commands
        self._add_task_resumption_commands()
    
    def _create_root_task_sql(self, task):
        """Generate SQL for root tasks (with schedule)"""
        warehouse_clause = f"WAREHOUSE = {self.warehouse}"
        schedule_clause = f"SCHEDULE = '{self.schedule}'"
        
        return f"""
        CREATE OR REPLACE TASK {task['name']}
            {warehouse_clause}
            {schedule_clause}
            AS
            {task['sql']};
        """
    
    def _create_dependent_task_sql(self, task):
        """Generate SQL for dependent tasks"""
        warehouse_clause = f"WAREHOUSE = {self.warehouse}"
        dependencies_clause = f"AFTER {', '.join(task['dependencies'])}"
        
        return f"""
        CREATE OR REPLACE TASK {task['name']}
            {warehouse_clause}
            {dependencies_clause}
            AS
            {task['sql']};
        """
    
    def _add_task_resumption_commands(self):
        """Add commands to resume all tasks"""
        for task in self.task_configs:
            self.task_sqls.append(f"ALTER TASK {task['name']} RESUME;")
    
    def get_task_graph_visualization(self):
        """Generate a dictionary representation of the task graph"""
        return {
            task['name']: {
                'dependencies': task['dependencies'],
                'is_root': not task['dependencies'],
                'is_final': task.get('is_final', False)
            }
            for task in self.task_configs
        }


class TaskGraphExecutor:
    def __init__(self, snowflake_connection):
        self.connection = snowflake_connection
    
    def execute_task_graph(self, task_sqls):
        """Execute the task creation SQLs in Snowflake"""
        results = []
        try:
            for i, sql in enumerate(task_sqls):
                cursor = self.connection.cursor()
                cursor.execute(sql)
                results.append({
                    "index": i,
                    "status": "success",
                    "sql": sql[:100] + "..." if len(sql) > 100 else sql
                })
                cursor.close()
            return {
                "status": "success", 
                "message": "Task graph created successfully",
                "task_results": results
            }
        except Exception as e:
            # Capture which task failed
            failed_task_index = len(results)
            failed_sql = task_sqls[failed_task_index] if failed_task_index < len(task_sqls) else "Unknown"
            failed_sql_preview = failed_sql[:100] + "..." if len(failed_sql) > 100 else failed_sql
            
            return {
                "status": "error", 
                "message": f"Error creating task graph: {str(e)}",
                "failed_at_task": failed_task_index,
                "failed_sql": failed_sql_preview,
                "task_results": results
            }


def SQLTaskGraphAgentInvoke():
    def invoke(state: Dict[str, Any]) -> Dict[str, Any]:
        agent = SQLTaskGraphAgent(
            warehouse=state.get("warehouse", "COMPUTE_WH"),
            schedule=state.get("schedule", "1 hour")
        )
        return agent(state)
    
    return invoke
