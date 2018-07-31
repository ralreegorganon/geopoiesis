﻿using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Dapper;

namespace CataclysmGis
{
    class Program
    {
        static void Main(string[] args)
        {
            var completeSql = @"
                with
                temp_landuse as 
                (
	                select 
		                pagenumber, LU05_DESC, lucode, percentage, 
		                max(percentage) over (partition by pagenumber) oid_max,
		                max(case when lu05_desc = 'Water' then 1 else 0 end) over (partition by pagenumber) water_precedence 
	                from om{0}_landuse_intersection_tabulation
                ), 
                max_landuse as
                (
	                select * from temp_landuse where (percentage = oid_max and water_precedence = 0) or (water_precedence = 1 and lu05_desc = 'Water')
                ),
                temp_road as 
                (
	                select pagenumber, rdtype, min(rdtype) over (partition by pagenumber) oid_max from om{0}_road_intersection_tabulation
                ), 
                max_road as
                (
	                select * from temp_road where rdtype = oid_max --and rdtype <=4
                ),
                attributed as
                (
	                select 
		                ml.pagenumber, 
		                case when rdtype is null then lucode else 999 end lucode,
		                case when rdtype is null then LU05_DESC else 'Road' end lu05_desc
	                from max_landuse ml left outer join max_road mr on ml.pagenumber = mr.pagenumber
                ),
                quantized as
                (
	                select 
		                otg.pagenumber, lucode, lu05_desc
	                from
		                om{0}_overmap_terrain_grid otg
		                left outer join attributed a
			                on otg.pagenumber = a.pagenumber
                ),
                translated as
                (
                select 
	                pagenumber,
	                lu05_desc,
	                case 
		                when lu05_desc = 'Brushland/Successional' then 'field'		
		                when lu05_desc = 'Cemetery' then 'cemetery_small_north'	
		                when lu05_desc = 'Commercial' then 's_gun'	
		                when lu05_desc = 'Cranberry Bog' then 'pond_swamp'
		                when lu05_desc = 'Cropland' then 'farm_1'	
		                when lu05_desc = 'Forest' then 'forest'	
		                when lu05_desc = 'Forested Wetland' then 'forest_water'	
		                when lu05_desc = 'Golf Course' then 'park'
		                when lu05_desc = 'High Density Residential' then 'house'		
		                when lu05_desc = 'Industrial' then 'small_storage_units'		
		                when lu05_desc = 'Junkyard' then 'toxic_dump'
		                when lu05_desc = 'Low Density Residential' then 'house'	
		                when lu05_desc = 'Marina' then 'fishing_pond_0_0'
		                when lu05_desc = 'Medium Density Residential' then 'house'	
		                when lu05_desc = 'Mining' then 'mine'
		                when lu05_desc = 'Multi-Family Residential' then 'apartments_con_tower_NE'
		                when lu05_desc = 'Non-Forested Wetland' then 'pond_swamp'	
		                when lu05_desc = 'Nursery' then 'orchard_tree_apple'
		                when lu05_desc = 'Open Land' then 'field'	
		                when lu05_desc = 'Orchard' then 'orchard_tree_apple'
		                when lu05_desc = 'Participation Recreation' then 'park'		
		                when lu05_desc = 'Pasture' then 'farm_1'
		                when lu05_desc = 'Powerline/Utility' then 'pwr_sub_s'	
		                when lu05_desc = 'Road' then 'road_ns' 
		                when lu05_desc = 'Saltwater Sandy Beach' then 'dirtlot'
		                when lu05_desc = 'Saltwater Wetland' then 'pond_swamp'
		                when lu05_desc = 'Spectator Recreation' then 'gym'
		                when lu05_desc = 'Transitional' then 'field'	
		                when lu05_desc = 'Transportation' then 'field'
		                when lu05_desc = 'Urban Public/Institutional' then 'police_north'	
		                when lu05_desc = 'Very Low Density Residential' then 'house' 
		                when lu05_desc = 'Waste Disposal' then 'sewage_treatment' 
		                when lu05_desc = 'Water' then 'river'     
		                when lu05_desc = 'Water-Based Recreation' then 'fishing_pond_0_0'		   
		                else 'river'
	                end	omt
	                from quantized
                )
                select omt from translated order by pagenumber
                ";

            using (var connection = new SqlConnection("Server=localhost;Database=Cataclysm;Trusted_connection=true"))
            {
                connection.Open();

                var oms = connection.Query(@"
                    select pagename, pagenumber, pagenumber/69 as y, pagenumber%69 as x from overmap_filtered_grid  
                    order by pagenumber
                    ");
                foreach (var om in oms)
                {
                    string sql = string.Format(completeSql, om.pagenumber);
                    var result = connection.Query(sql);

                    OvermapTerrain current = null;

                    var list = new List<OvermapTerrain>();

                    var derp = 0;
                    foreach (var r in result)
                    {
                        derp++;
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

                            current = new OvermapTerrain {
                                Type = r.omt,
                                Count = 1
                            };
                        }
                    }
                    list.Add(current);

                    var inner = string.Join(",", list.Select(x => $"[\"{x.Type}\",{x.Count}]"));
                    var outer = $",[{inner}]";

                    var template = File.ReadAllLines("omtemplate.txt");
                    template[12] = outer;

                    File.WriteAllLines($@"F:\code\cpp\Cataclysm-DDA\save\Hacks\o.{om.x}.{om.y}", template);
                }
            }
        }
    }

    public class OvermapTerrain
    {
        public string Type { get; set; }
        public int Count { get; set; }
    }
}
