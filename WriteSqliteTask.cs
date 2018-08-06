using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using Dapper;
using Microsoft.Data.Sqlite;

namespace geopoiesis
{
    public class WriteSqliteTask
    {
        private string sqlConnectionString;
        private string sqlitePath;
        private int[] overmapSubsetIds;

        private List<(string field, string type)> fields = new List<(string field, string type)> {
            ("om_pagenumber", "int"),
            ("omt_pagenumber", "int"),
            ("om_y", "int"),
            ("om_x", "int"),
            ("omt_y", "int"),
            ("omt_x", "int"),
            ("land_use_code", "int"),
            ("primary_road_type", "int"),
            ("all_road_types", "text"),
            ("subway_primary_line", "text"),
            ("subway_all_lines", "text"),
            ("rapid_transit_primary_line", "text"),
            ("rapid_transit_primary_line_grade", "int"),
            ("rapid_transit_all_lines", "text"),
            ("rapid_transit_all_lines_grades", "text"),
            ("transit_station_line", "text"),
            ("transit_station_is_terminus", "int"),
            ("military_base_component", "text"),
            ("military_base_name", "text"),
            ("town_id", "int"),
            ("trail_class", "int"),
            ("train_type", "int"),
            ("station", "int"),
            ("ocean", "int"),
            ("primary_structure_id", "text"),
            ("primary_structure_total_area_sq_m", "real"),
            ("primary_structure_other_cell_cover_count", "int"),
            ("cell_structure_count", "int"),
            ("cell_structure_total_percentage_covered", "real"),
        };

        public WriteSqliteTask(string sqlConnectionString, string sqlitePath, int[] overmapSubsetIds)
        {
            this.sqlConnectionString = sqlConnectionString;
            this.sqlitePath = sqlitePath;
            this.overmapSubsetIds = overmapSubsetIds;
        }

        public void Execute()
        {
            File.Delete(sqlitePath);

            using (var sourceConnection = new SqlConnection(sqlConnectionString))
            using (var targetConnection = new SqliteConnection($"Data Source={sqlitePath}"))
            {
                sourceConnection.Open();
                targetConnection.Open();

                var createColumns = string.Join(",", fields.Select(x => $"{x.field} {x.type}"));
                var createTable = $"create table omt ({createColumns})";
                targetConnection.Execute(createTable);

                using (var cmd = targetConnection.CreateCommand())
                {
                    var insertColumns = string.Join(",", fields.Select(x => x.field));
                    var insertValues = string.Join(",", fields.Select(x => $"${x.field}"));
                    var insert = $"insert into omt ({insertColumns}) values ({insertValues})";
                    cmd.CommandText = insert;

                    var parameters = fields.Select(x => {
                            var p = cmd.CreateParameter();
                            p.ParameterName = $"${x.field}";
                            return p;
                        })
                        .ToList();

                    cmd.Parameters.AddRange(parameters);
                    cmd.Prepare();

                    var query = $@"
                        select pagename, pagenumber, pagenumber/69 as y, pagenumber%69 as x from overmap_subset_grid
                        {(overmapSubsetIds.Length == 0 ? "" : string.Format("where pagenumber in ({0})", string.Join(",", overmapSubsetIds)))}
                        order by pagenumber
                    ";
                    var oms = sourceConnection.Query(query);

                    foreach (var om in oms)
                    {
                        using (var tx = targetConnection.BeginTransaction())
                        {
                            cmd.Transaction = tx;
                            var sources = sourceConnection.Query($"select * from attributes_om{om.pagenumber} order by om_x, om_y");

                            foreach (var source in sources)
                            {
                                var values = ((IDictionary<string, object>) source).Values.ToList();

                                for (var i = 0; i < parameters.Count; i++)
                                {
                                    parameters[i].Value = values[i] ?? DBNull.Value;
                                }
                                cmd.ExecuteNonQuery();
                            }
                            tx.Commit();
                        }
                    }

                    targetConnection.Execute("create index idx_omt_omxy on omt (om_x, om_y)");
                }
            }
        }
    }
}
