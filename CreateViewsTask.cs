using System.Data.SqlClient;
using Dapper;

namespace geopoiesis
{
    public class CreateViewsTask
    {
        private readonly string sqlConnectionString;
        private readonly int[] overmapSubsetIds;
        private string viewSqlTemplate => @"
            create or alter view attributes_om{0} as
            with
            rank_landuse2005_poly as 
            (
	            select 
		            pagenumber, 
		            lucode land_use_code,
		            rank() over (partition by pagenumber order by case when lucode = 20 then 999 else percentage end) r
	            from 
		            tabulation_om{0}_landuse2005_poly
            ),
            choose_landuse2005_poly as
            (
	            select 
		            pagenumber,
		            land_use_code
	            from 
		            rank_landuse2005_poly 
	            where 
		            r = 1
            ),
            rank_and_choose_eotroads_arc as
            (
	            select 
		            pagenumber,
		            min(rdtype) primary_road_type,
		            string_agg(rdtype, ',') all_road_types
	            from 
		            tabulation_om{0}_eotroads_arc
	            group by 
		            pagenumber
            ),
            rank_mbta_arc_subway as
            (
	            select 
		            pagenumber,
		            line,
		            rank() over (partition by pagenumber order by length desc) r
	            from
		            tabulation_om{0}_mbta_arc
	            where 
		            grade = 7
            ),
            choose_mbta_arc_subway as
            (
	            select 
		            pagenumber,
		            min(case when r = 1 then line else null end) subway_primary_line,
		            string_agg(line, ',') subway_all_lines
	            from 
		            rank_mbta_arc_subway
	            group by 
		            pagenumber
            ),
            rank_mbta_arc_nosubway as
            (
	            select 
		            pagenumber,
		            line,
		            grade,
		            rank() over (partition by pagenumber order by length desc) r
	            from
		            tabulation_om{0}_mbta_arc
	            where 
		            grade <> 7
            ),
            choose_mbta_arc_nosubway as
            (
	            select 
		            pagenumber,
		            min(case when r = 1 then line else null end) rapid_transit_primary_line,
		            min(case when r = 1 then grade else null end) rapid_transit_primary_line_grade,
		            string_agg(line, ',') rapid_transit_all_lines,
		            string_agg(grade, ',') rapid_transit_all_lines_grades
	            from 
		            rank_mbta_arc_nosubway
	            group by 
		            pagenumber
            ),
            rank_and_choose_mbta_node as
            (
	            select 
		            pagenumber, 
		            max(case when terminus = 'Y' then 1 else 0 end) transit_station_is_terminus, 
		            string_agg(line, '/') transit_station_line 
	            from 
		            tabulation_om{0}_mbta_node 
	            group by 
		            pagenumber 
            ),
            rank_military_bases as 
            (
	            select 
		            pagenumber, 
		            component,
		            site_name,
		            rank() over (partition by pagenumber order by site_name desc) as r
	            from 
		            tabulation_om{0}_military_bases
            ),
            choose_military_bases as
            (
	            select 
		            pagenumber,
		            component military_base_component,
		            site_name military_base_name
	            from 
		            rank_military_bases 
	            where 
		            r = 1
            ),
            rank_towns as 
            (
	            select 
		            pagenumber, 
		            town_id,
		            rank() over (partition by pagenumber order by percentage desc) as r
	            from 
		            tabulation_om{0}_townssurvey_polym
            ),
            choose_towns as
            (
	            select 
		            pagenumber,
		            town_id
	            from 
		            rank_towns 
	            where 
		            r = 1
            ),
            rank_trails as 
            (
	            select 
		            pagenumber, 
		            class,
		            rank() over (partition by pagenumber order by length desc) as r
	            from 
		            tabulation_om{0}_trails_arc
            ),
            choose_trails as
            (
	            select 
		            pagenumber,
		            class trail_class
	            from 
		            rank_trails 
	            where 
		            r = 1
            ),
            rank_trains as 
            (
	            select 
		            pagenumber, 
		            type,
		            rank() over (partition by pagenumber order by length desc, type) as r
	            from 
		            tabulation_om{0}_trains_arc 
	            where 
		            type <> 9
            ),
            choose_trains as
            (
	            select 
		            pagenumber,
		            type train_type
	            from 
		            rank_trains 
	            where 
		            r = 1
            ),
            rank_and_choose_trains_node as 
            (
	            select 
		            distinct pagenumber, 
		            1 station
	            from 
		            tabulation_om{0}_trains_node 
            ), 
            rank_and_choose_ocean_poly as
            (
	            select
		            pagenumber
	            from
		            tabulation_om{0}_oceanmask_poly
            ),
            rank_structure_poly as
            ( 
	            select 
		            pagenumber,
		            struct_id,
		            area_sq_ft / 10.764 area_sq_m,
		            count(*) over (partition by struct_id) primary_structure_other_cell_cover_count,
		            count(*) over (partition by pagenumber) cell_structure_count, 
		            sum(percentage) over (partition by pagenumber) cell_structure_total_percentage_covered, 
		            rank() over (partition by pagenumber order by area_sq_ft desc) ranking 
	            from 
		            tabulation_om{0}_structure_poly 
            ),
            choose_structure_poly as
            (
	            select 
		            pagenumber,
		            struct_id primary_structure_id,
		            area_sq_m primary_structure_total_area_sq_m,
		            primary_structure_other_cell_cover_count,
		            cell_structure_count,
		            cell_structure_total_percentage_covered
	            from 
		            rank_structure_poly 
	            where 
		            ranking = 1
            ),
            attributed as 
            (
	            select 
                    {0} om_pagenumber,
		            otg.pagenumber omt_pagenumber,
		            {0}/69 om_y,
		            {0}%69 om_x,
		            otg.pagenumber/180 omt_y,
		            otg.pagenumber%180 omt_x,
		            lu.land_use_code,
		            r.primary_road_type,
		            r.all_road_types,
		            mbtas.subway_primary_line,
		            mbtas.subway_all_lines,
		            mbtans.rapid_transit_primary_line,
		            mbtans.rapid_transit_primary_line_grade,
		            mbtans.rapid_transit_all_lines,
		            mbtans.rapid_transit_all_lines_grades,
		            mbtan.transit_station_line,
		            mbtan.transit_station_is_terminus,
		            mb.military_base_component,
		            mb.military_base_name,
		            t.town_id,
		            tr.trail_class,
		            trz.train_type,
		            trzn.station,
		            case when lu.land_use_code is null and op.pagenumber is not null then 1 else 0 end as ocean,
		            sp.primary_structure_id,
		            sp.primary_structure_total_area_sq_m,
		            sp.primary_structure_other_cell_cover_count,
		            sp.cell_structure_count,
		            sp.cell_structure_total_percentage_covered
	            from 
		            overmap_terrain_grid_om{0} otg
		            left outer join choose_landuse2005_poly lu
			            on otg.pagenumber = lu.pagenumber
		            left outer join rank_and_choose_eotroads_arc r
			            on otg.pagenumber = r.pagenumber
		            left outer join choose_mbta_arc_subway mbtas
			            on otg.pagenumber = mbtas.pagenumber
		            left outer join choose_mbta_arc_nosubway mbtans
			            on otg.pagenumber = mbtans.pagenumber
		            left outer join rank_and_choose_mbta_node mbtan
			            on otg.pagenumber = mbtan.pagenumber
		            left outer join choose_military_bases mb
			            on otg.pagenumber = mb.pagenumber
		            left outer join choose_towns t
			            on otg.pagenumber = t.pagenumber
		            left outer join choose_trails tr
			            on otg.pagenumber = tr.pagenumber
		            left outer join choose_trains trz
			            on otg.pagenumber = trz.pagenumber
		            left outer join rank_and_choose_trains_node trzn
			            on otg.pagenumber = trzn.pagenumber
		            left outer join rank_and_choose_ocean_poly op
			            on otg.pagenumber = op.pagenumber
		            left outer join choose_structure_poly sp
			            on otg.pagenumber = sp.pagenumber
            )
            select * from attributed
            ";

        public CreateViewsTask(string sqlConnectionString, int[] overmapSubsetIds)
        {
            this.sqlConnectionString = sqlConnectionString;
            this.overmapSubsetIds = overmapSubsetIds;
        }

        public void Execute()
        {
            var query = $@"
                select pagename, pagenumber, pagenumber/69 as y, pagenumber%69 as x from overmap_subset_grid
                {(overmapSubsetIds.Length == 0 ? "" : string.Format("where pagenumber in ({0})", string.Join(",", overmapSubsetIds)))}
                order by pagenumber
            ";
            using (var c = new SqlConnection(sqlConnectionString))
            {
                c.Open();
                var oms = c.Query(query);

                foreach (var om in oms)
                {
                    var sql = string.Format(viewSqlTemplate, (int) om.pagenumber);
                    c.Execute(sql);
                }
            }
        }
    }
}