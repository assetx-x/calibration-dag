from luigi import Task

from base import DCMTaskParams


class DiscoverRulesTask(DCMTaskParams, Task):
    def run(self):
        mock = """udf_name,offset,sector,industry,iq_mktcap_total_rev_sector_date,q4_sector_date,iq_pe_excl_sector_date,iq_dilut_eps_excl_sector_date
['get_return'],['-11B'],['Consumer Goods'],"['Trucks &amp; Other Vehicles','Meat Products','Personal Products','Home Furnishings &amp; Fixtures', 'Farm Products','Textile - Apparel Clothing','Food - Major Diversified']","[0,1,2,3]","[5,6,7]",,
['get_return'],['-11B'],['Consumer Goods'],"['Trucks &amp; Other Vehicles','Meat Products','Personal Products','Home Furnishings &amp; Fixtures', 'Farm Products','Textile - Apparel Clothing','Food - Major Diversified']","[0,1,2,3]","[5,6,7]","[0,1]",
['get_return'],['-11B'],['Consumer Goods'],"['Trucks &amp; Other Vehicles','Meat Products','Personal Products','Home Furnishings &amp; Fixtures', 'Farm Products','Textile - Apparel Clothing','Food - Major Diversified']","[0,1,2,3]","[6,7]","[0,1]",
['get_return'],['-11B'],['Consumer Goods'],"['Trucks &amp; Other Vehicles','Meat Products','Personal Products','Home Furnishings &amp; Fixtures', 'Farm Products','Textile - Apparel Clothing','Food - Major Diversified']","[0,1,2]","[5,6,7]",,
['get_return'],['-11B'],['Consumer Goods'],"['Trucks &amp; Other Vehicles','Meat Products','Personal Products','Home Furnishings &amp; Fixtures', 'Farm Products','Textile - Apparel Clothing','Food - Major Diversified']","[0,1,2]","[6,7]",,
['get_return'],['-11B'],['Consumer Goods'],"['Trucks &amp; Other Vehicles','Meat Products','Personal Products','Home Furnishings &amp; Fixtures', 'Farm Products','Textile - Apparel Clothing','Food - Major Diversified']","[0,1,2]","[6,7]","[0,1]",
['get_return'],['-11B'],['Consumer Goods'],,"[0,1,2]","[6,7]",[0],
['get_return'],['-11B'],['Consumer Goods'],"['Trucks &amp; Other Vehicles','Meat Products','Personal Products','Home Furnishings &amp; Fixtures', 'Farm Products','Textile - Apparel Clothing','Food - Major Diversified']","[0,1,2]","[6,7]",[0],
['get_return'],['-11B'],['Consumer Goods'],,"[0,1,2]",[7],[0],
['get_return'],['-11B'],['Consumer Goods'],"['Trucks &amp; Other Vehicles','Meat Products','Personal Products','Home Furnishings &amp; Fixtures', 'Farm Products','Textile - Apparel Clothing','Food - Major Diversified']","[0,1,2]",[7],[0],
['get_return'],['-11B'],['Consumer Goods'],"['Trucks &amp; Other Vehicles','Meat Products','Personal Products','Home Furnishings &amp; Fixtures', 'Farm Products','Textile - Apparel Clothing','Food - Major Diversified']","[0,1]","[6,7]",,
['get_return'],['-11B'],['Industrial Goods'],,,"[6,7]","[4,5,6,7]","[2,3,4]"
['get_return'],['-11B'],['Sevices'],"['Gaming Activities','Resorts &amp; Casinos','Drug Stores','Regional Airlines','Sporting Activities','Railroads', 'Advertising Agencies','Home Furnishing Stores','Specialty Eateries','CATV Systems','Industrial Equipment Wholesale']","[0,1]",,[4],
['get_return'],['-11B'],['Sevices'],"['Gaming Activities','Resorts &amp; Casinos','Drug Stores','Regional Airlines','Sporting Activities','Railroads', 'Advertising Agencies','Home Furnishing Stores','Specialty Eateries','CATV Systems','Industrial Equipment Wholesale']","[0,1]",[7],"[2,3,4]",
['get_return'],['-11B'],['Technology'],"['Computer Based Systems','Semiconductor- Memory Chips','Technical &amp; System Software','Processing Systems &amp; Products','Semiconductor - Broad Line','Wireless Communications']","[0,1]","[5,6,7]",,
['get_return'],['-11B'],['Basic Material'],['Specialty Chemicals'],,"[0,1,2,6,7]",,
['get_return'],['-11B'],['Basic Material'],['Specialty Chemicals'],"[0,1]","[0,1,2,6,7]",,
['get_return'],['-11B'],['Basic Material'],['Oil'],,,,
['get_return'],['-11B'],['Basic Material'],['Oil'],,,,
['get_return'],['-11B'],['HealthCare'],"['Medical Instruments &amp; Supplies','Medical Appliances &amp; Equipment']","[0,1]","[0,1,2,6,7]","[0,1,2,3]","[1,2,3,4]"
"""

        with self.output().open("w") as output_fd:
            output_fd.write(mock)