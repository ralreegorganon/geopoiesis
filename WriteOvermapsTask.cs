using System.Collections.Generic;
using System.IO;
using System.Linq;
using Dapper;
using Microsoft.Data.Sqlite;

namespace geopoiesis
{
    public class WriteOvermapsTask
    {
        private string sqlitePath;
        private string overmapOutputPath;

        private string subwaySql = @"
            select 
                om_x,
                om_y,
                omt_x,
                omt_y,
                case 
    	            when transit_station_line is not null and subway_primary_line is not null then 'sub_station'
    	            when subway_primary_line is not null then 'subway_east'
                    else 'empty_rock'
                end as omt 
            from omt  
            where om_x = 66 and om_y = 42
            order by om_y, om_x, omt_y, omt_x
        ";

        private string groundSql = @"
            with
            translated as
            (
                select 
                    *,
                    case 
                    when land_use_code = 40 then 'field'		
                    when land_use_code = 34 then 'cemetery_small_north'	
                    when land_use_code = 15 then 's_gun_north'	
                    when land_use_code = 23 then 'pond_swamp'
                    when land_use_code = 1 then 'farm_1'	
                    when land_use_code = 3 then 'forest'	
                    when land_use_code = 37 then 'forest_water'	
                    when land_use_code = 26 then 'park'
                    when land_use_code = 11 then 'house_north'		
                    when land_use_code = 16 then 'small_storage_units_north'		
                    when land_use_code = 39 then 'toxic_dump'
                    when land_use_code = 13 then 'house_north'	
                    when land_use_code = 29 then 'fishing_pond_0_0_north'
                    when land_use_code = 12 then 'house_north'	
                    when land_use_code = 5 then 'mine'
                    when land_use_code = 10 then 'apartments_con_tower_NE_north'
                    when land_use_code = 4 then 'pond_swamp'	
                    when land_use_code = 36 then 'orchard_tree_apple'
                    when land_use_code = 6 then 'field'	
                    when land_use_code = 35 then 'orchard_tree_apple'
                    when land_use_code = 7 then 'park'		
                    when land_use_code = 2 then 'farm_1_north'
                    when land_use_code = 24 then 'pwr_sub_s'	
                    when land_use_code = 25 then 'dirtlot'
                    when land_use_code = 14 then 'pond_swamp'
                    when land_use_code = 8 then 'gym'
                    when land_use_code = 17 then 'field'	
                    when land_use_code = 18 then 'spiral'
                    when land_use_code = 31 then 'police_north'	
                    when land_use_code = 38 then 'house_north' 
                    when land_use_code = 19 then 'sewage_treatment' 
                    when land_use_code = 20 then 'river_north'     
                    when land_use_code = 9 then 'fishing_pond_0_0_north'		   
                    end	omt,
                    case
                        when primary_road_type is not null then 'road_ns'
                    end road
                from 
                    omt
            )
            select 
                om_x,
                om_y,
                omt_x,
                omt_y,
                case 
    	            when trail_class is not null then 'NatureTrail_1a_north'
                    when transit_station_line is not null and subway_primary_line is not null then 'sub_station_north'
    	            when transit_station_line is not null then 'tower_lab_finale'
    	            when train_type in (1, 2) then 'bank_north'
                    when train_type is not null then 'field'
    	            when station = 1 then 'lab_train_depot'
    	            when primary_road_type is not null then road 
    	            when military_base_name is not null then 'bunker_north'
    	            when omt is not null then omt
    	            when ocean = 1 then 'river_north'
    	            else 'forest'
	            end as omt 
            from 
                translated
            where 
                om_x = 66 and om_y = 42
            order by 
                om_y, om_x, omt_y, omt_x
        ";

        public WriteOvermapsTask(string sqlitePath, string overmapOutputPath)
        {
            this.sqlitePath = sqlitePath;
            this.overmapOutputPath = overmapOutputPath;
        }

        public void Execute()
        {
            using (var c = new SqliteConnection($"Data Source={sqlitePath}"))
            {
                var z9 = c.Query<OvermapTerrain>(subwaySql);
                var z10 = c.Query<OvermapTerrain>(groundSql);

                var z9overmaps = z9.GroupBy(x => (x.om_x, x.om_y)).ToDictionary(k => k.Key, v => v.ToList());
                var z10overmaps = z10.GroupBy(x => (x.om_x, x.om_y)).ToDictionary(k => k.Key, v => v.ToList());

                foreach (var key in z10overmaps.Keys)
                {
                    var z9om = z9overmaps[key];
                    var z10om = z10overmaps[key];

                    var z9Text = OvermapTerrainToText(z9om);
                    var z10Text = OvermapTerrainToText(z10om);

                    var template = File.ReadAllLines("omtemplate.txt");
                    template[11] = z9Text;
                    template[12] = z10Text;

                    File.WriteAllLines(string.Format(overmapOutputPath, key.Item1, key.Item2), template);
                }
            }
        }

        private string OvermapTerrainToText(List<OvermapTerrain> omt)
        {
            OvermapTerrainGroup current = null;

            var list = new List<OvermapTerrainGroup>();

            foreach (var r in omt)
            {
                if (r.omt == current?.Type)
                {
                    current.Count++;
                }
                else
                {
                    if (current != null)
                    {
                        list.Add(current);
                    }

                    current = new OvermapTerrainGroup
                    {
                        Type = r.omt,
                        Count = 1
                    };
                }
            }
            list.Add(current);

            var inner = string.Join(",", list.Select(x => $"[\"{x.Type}\",{x.Count}]"));
            var outer = $",[{inner}]";

            return outer;
        }
    }

    public class OvermapTerrainGroup
    {
        public string Type { get; set; }
        public int Count { get; set; }
    }

    public class OvermapTerrain
    {
        public int om_x { get; set; }
        public int om_y { get; set; }
        public int omt_x { get; set; }
        public int omt_y { get; set; }
        public string omt { get; set; }
    }
}
