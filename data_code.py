from datetime import datetime
from typing import Iterable
from scrapy.cmdline import execute
import scrapy
import json
import pymysql
import re
from scrapy import Request
import os
from urllib.parse import unquote

class DataSpider(scrapy.Spider):
    name = "data_new"
    allowed_domains = ["www.grpumps.com"]
    # start_urls = ["https://www.grpumps.com/product"]
    main_list = list()
    file_date = datetime.now().strftime("%d%m%Y")
    def insert_into_db(self, category_item):
        try:
            conn = pymysql.connect(
                host="localhost",
                user="root",
                password="actowiz",
                database="grpumps",
                charset="utf8mb4"
            )
            cursor = conn.cursor()

            sql = f"""
            INSERT INTO grpumps_data_{self.file_date} (
                 name, model_id, secondary_identifiers, manufacturer, description, short_description,
                extra_textual_info, technical_specs, bom, manuals, images, attachments,
                related_models, breadcrumbs, prices, warranty_info, lifecycle_status,
                release_date, end_of_life_date, compatibility, certifications,
                market_region, metadata
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                category_item.get("name"),
                category_item.get("model_id"),
                json.dumps(category_item.get("secondary_identifiers")),  # JSON
                json.dumps(category_item.get("manufacturer")),  # JSON
                category_item.get("description"),  # TEXT
                category_item.get("short_description"),  # TEXT
                category_item.get("extra_textual_info"),  # TEXT
                json.dumps(category_item.get("technical_specs")),  # JSON
                json.dumps(category_item.get("bom")),  # JSON
                json.dumps(category_item.get("manuals")),  # JSON
                json.dumps(category_item.get("images")),  # JSON
                json.dumps(category_item.get("attachments")),  # JSON
                json.dumps(category_item.get("related_models")),  # JSON
                json.dumps(category_item.get("breadcrumbs")),  # JSON
                json.dumps(category_item.get("prices")),  # JSON
                json.dumps(category_item.get("warranty_info")),  # JSON
                json.dumps(category_item.get("lifecycle_status")),  # JSON
                category_item.get("release_date"),  # VARCHAR
                category_item.get("end_of_life_date"),  # VARCHAR
                json.dumps(category_item.get("compatibility")),  # JSON
                json.dumps(category_item.get("certifications")),  # JSON
                category_item.get("market_region"),  # VARCHAR
                json.dumps(category_item.get("metadata")),  # JSON
            )

            cursor.execute(sql, values)
            conn.commit()
            cursor.close()
            conn.close()
            print(f"✅ Inserted: {category_item.get('name')}")
        except Exception as e:
            print(f"❌ DB insert error: {e}")


    def safe_decode_url(self, text):
        """
        Decode percent-encoded text (like %C2%AE → ®) only if needed.
        If already decoded, return as-is.
        """
        if not text:
            return text

        # Only decode if it actually contains encoded patterns like %xx
        if re.search(r'%[0-9A-Fa-f]{2}', text):
            return unquote(text)
        return text

    def remove_extra_spaces(self,text):
        cleaned_text = re.sub(r'\s+', ' ', text)
        return cleaned_text.strip()

    def create_productid(self,text):
        text = text.lower().strip()
        # Replace non-alphanumeric characters with '-'
        text = re.sub(r'[^a-z0-9]+', '-', text)
        # Remove leading/trailing hyphens
        text = text.strip('-')
        return text

    def start_requests(self):
        with open(fr"D:\Mansi\Other_Project\GRPumps\GRPumps\grpumps_final_mapping.json","r",encoding="UTF-8") as data:
            json_response = json.load(data)
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-US,en;q=0.9',
                'cache-control': 'max-age=0',
                'priority': 'u=0, i',
                'referer': 'https://www.grpumps.com/product',
                'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-user': '?1',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
                # 'cookie': '_gcl_au=1.1.395168776.1761309123; _ga=GA1.1.1072716952.1761309124; CAKEPHP=4i173jcaeq3fv1ne4mrhsqkbbb; chatbase_anon_id=e1663f06-f1a3-4012-8e0f-25e9c664829e; _ga_6T7VQY5Q6T=GS2.1.s1761986887$o9$g1$t1761987391$j60$l0$h995304018',
            }
            for item in json_response:
                for cat_name, cat_data in item.items():
                    cat_url = cat_data.get("cat_url","")
                    # if cat_url=="http://www.grpumps.com/product/pump/S-Series-Slimline-Submersible":
                    model_urls = cat_data.get("model_urls",[])
                    print("cat_url",cat_url)
                    # print(model_urls)
                    model_id = self.safe_decode_url(cat_url.split("/")[-1].replace("-", "_"))
                    os.makedirs( fr"D:\PAGESAVES\GRPumps_pagesave",exist_ok=True)
                    local_path = fr"D:\PAGESAVES\GRPumps_pagesave\{model_id}.html"

                    # if os.path.exists(local_path):
                    #     file_url = f"file:///{local_path.replace(os.sep,'/')}"
                    #     print("page reading....")
                    #     yield scrapy.Request(url=file_url,cb_kwargs={"cat_url":cat_url,"model_urls":model_urls,"cat_name":cat_name},callback=self.parse)
                    # else:
                    # if cat_url=="http://www.grpumps.com/product/pump/O-Series":
                    yield scrapy.Request(url=cat_url,headers=headers,cb_kwargs={"cat_url":cat_url,"model_urls":model_urls,"cat_name":cat_name},callback=self.parse)
    def parse(self, response,**kwargs):
        category_item = dict()
        model_urls = kwargs["model_urls"]
        cat_name = kwargs["cat_name"]
        if response.status == 200:
            name = self.remove_extra_spaces("".join(response.xpath('//div[@class="grbase-subheader"]//h3//text()').getall()).strip())
            id_ = self.create_productid(name.lower())
            category_item["name"] = name
            model_id = self. safe_decode_url(response.url.split("/")[-1].replace("-","_"))
            category_item["model_id"] = self.create_productid(name)
            with open ( fr"D:\PAGESAVES\GRPumps_pagesave\{model_id}.html","w") as f:
                f.write(response.text)
            # secondary_identifiers
            category_item["secondary_identifiers"] = []
            # manufacturer
            manufacturer= {"name":"Gorman-Rupp Pumps",
            "description": "©1998-2025 Gorman-Rupp Pumps - A Subsidiary of The Gorman-Rupp Company, All Rights Reserved.",
            "website": "https://www.grpumps.com/",
            "logo_url": "https://www.grpumps.com/img/pump_group/GR_logo-base_PP.png",
            "founded_year": 1933,
            "headquarters": "Mansfield, Ohio, USA"}
            category_item["manufacturer"] = manufacturer
            #description
            description = "".join(response.xpath('//div[@class="grbase-bodycontent"]//div[@class="col-md-12 grbase-body-block"]//div[@class="row"][1]/div[1]//text()').getall()).strip()
            description_2 = response.xpath('//h4[@class="page-header-sidebar"][contains(text(),"FEATURES")]/following-sibling::div[@class="sidebar-content"]//text()').getall()
            if description_2:
                description_full = description+"".join(description_2).strip()
            else:
                description_full = description
            category_item["description"] =self.remove_extra_spaces(description_full)

            #short_description
            category_item["short_description"] = None
            # #extra_textual_info
            # if model_urls and model_urls != []:
            #     category_item["extra_textual_info"] = "Consult your Gorman-Rupp distributor for detailed information concerning your model. Parts lists can sometimes vary from pump to pump.  Your serial number is critical in determining the correct parts and service requirements for your pump. Information on this page is provided for reference only."
            # else:
            category_item["extra_textual_info"] = None
            #technical_specs
            try:
                spcification_list = list()
                technical_specs = response.xpath('//h4[@class="page-header"][contains(text(),"Specifications")]/following-sibling::table')
                if technical_specs:
                    table = technical_specs.xpath('.//tbody//tr')
                    for data in table:
                        known_units = {"mm", "m", "cm", "in", "SSU","cST","C", "F", "GPM", "lps", "psi", "PSI","bar","BAR", "Hz", "kW", "HP","ft","%","lb","V","P"}

                        value_text = data.xpath('.//td[2]/text()').get() or ""
                        units = [
                            u.replace('"', 'in')
                            for u in dict.fromkeys(re.findall(r'[°]?[A-Za-z%"]+', value_text))
                            if u.replace('"', 'in') in known_units
                        ]
                        spcification_list.append({"display_name":data.xpath('.//td[1]/text()').get(),
                                "value":data.xpath('.//td[2]/text()').get(),
                                "unit": ",".join(units) if units else None,
                                "description":None})
                category_item["technical_specs"] = spcification_list if spcification_list else []
            except Exception as e:
                print(str(e))
                technical_specs = None
                category_item["technical_specs"] = None

            #bom
            category_item["bom"] = []
            #manuals
            manual_ext = ['.pdf', '.doc', '.docx']
            # attachment_ext = ['.gif', '.jpg', '.jpeg', '.png', '.mp4', '.mov', '.avi', '.zip', '.stp', '.dwg']
            try:
                Manual = list()
                manual_text = response.xpath('//h4[@class="page-header-sidebar"][contains(text(),"RESOURCES")]/following-sibling::div[@class="sidebar-links"]/a')
                for menu in manual_text:
                    menu_dict=dict()
                    mtext = menu.xpath(".//text()").get()
                    murl = menu.xpath(".//@href").get()
                    if "https://assets.grpumps.com/" in murl:
                        pass
                    else:
                        murl = "https://assets.grpumps.com"+murl

                    for manualloop in manual_ext:
                        if murl.endswith(manualloop):
                            menu_dict["name"] = mtext
                            menu_dict["description"] = None
                            menu_dict["manufacturer"] = None
                            menu_dict["publication_date"] = None
                            menu_dict["version"] = None
                            menu_dict["metadata"] = {"collected_at":datetime.now().isoformat(),"data_source_url":self.safe_decode_url(murl)}
                            Manual.append(menu_dict)

                category_item["manuals"] = Manual
            except Exception as e:
                print(str(e))
                category_item["manuals"] = []


            #images
            imags = response.xpath('//img[@title]/@src').getall()
            category_item["images"] = imags

            # attachments
            attachment_ext = ['.gif', '.jpg', '.jpeg', '.png', '.mp4', '.mov', '.avi', '.zip', '.stp', '.dwg']
            try:
                att_list= list()
                att_text = response.xpath('//h4[@class="page-header-sidebar"][contains(text(),"RESOURCES")]/following-sibling::div[@class="sidebar-links"]/a')
                for att in att_text:
                    att_dict=dict()
                    atext = att.xpath(".//text()").get()
                    aurl = att.xpath(".//@href").get()
                    if "https://assets.grpumps.com/" in aurl:
                        pass
                    else:
                        aurl = "https://assets.grpumps.com"+aurl
                    for attloop in attachment_ext:
                        if aurl.endswith(attloop):
                            att_dict["name"] = atext
                            att_dict["description"] = None
                            att_dict["file_type"] = attloop.replace(".","")
                            att_dict["metadata"] = {"collected_at":datetime.now().isoformat(),"data_source_url":self.safe_decode_url(aurl)}
                            att_list.append(att_dict)
                    if "Video" in atext:
                            att_dict["name"] = atext
                            att_dict["description"] = None
                            att_dict["file_type"] = "video"
                            att_dict["metadata"] = {"collected_at":datetime.now().isoformat(),"data_source_url": self.safe_decode_url(aurl)}
                            att_list.append(att_dict)

                category_item["attachments"] = att_list
            except Exception as e:
                print(str(e))
                category_item["attachments"] = None

            #related_models
            category_item["related_models"] = []
            #-------pending-----
            #breadcrumbs
            breadcrumbs_list = ["product","pump",name]
            category_item["breadcrumbs"] = breadcrumbs_list

            #prices
            category_item["prices"] = []

            #warranty_info
            warranty_info = None
            category_item["warranty_info"] = warranty_info

            #lifecycle_status
            lifecycle_status = None
            category_item["lifecycle_status"] = lifecycle_status

            #release_date
            release_date = None
            category_item["release_date"] = None

            #end_of_life_date
            end_of_life_date = None
            category_item["end_of_life_date"] = end_of_life_date

            #compatibility
            category_item["compatibility"] = []

            #certifications
            category_item["certifications"] = []


            #market_region
            market_region = "US"
            category_item["market_region"] = market_region

            #metadata
            metadata = {"collected_at":datetime.now().isoformat(),
            "data_source_url":self.safe_decode_url(response.url),
            "original_languages":[{"language_code":"en",
                                   "language_script_code":"latin",
                                   "region_code":"US"}
                                  ]}
            category_item["metadata"] = metadata
            # print(category_item)

            if model_urls:
                pass
                headers_murl = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept-language': 'en-US,en;q=0.9',
                    'cache-control': 'max-age=0',
                    'priority': 'u=0, i',
                    'referer': f'{response.url}',
                    'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'same-origin',
                    'sec-fetch-user': '?1',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
                }
                for m_url in model_urls:
                    category_item["related_models"].append(self.safe_decode_url(m_url.split("/")[-1]))
                if len(category_item["related_models"]) == len(model_urls):
                    print("updated model_urls in release data")
                for m_url in model_urls:
                    category_item["related_models"].append(self.safe_decode_url(m_url.split("/")[-1]))
                    # print(category_item)
                    # if model_urls == "http://www.grpumps.com/product/pump_model/O-Series/08D1-GHH":
                    yield scrapy.Request(url=m_url,headers=headers_murl,callback=self.parse_modelURL,dont_filter=True,meta={"category_item":category_item, "total": len(model_urls)})

                self.main_list.append(category_item)
                self.insert_into_db(category_item)
            else:
                self.main_list.append(category_item)
                self.insert_into_db(category_item)

    def parse_modelURL(self,response):
        model_dict = dict()
        category_item = response.meta.get("category_item")
        model_id =  self.safe_decode_url(response.url.split("/")[-1])
        os.makedirs(fr"D:\PAGESAVES\GRPumps_pagesave\{category_item["model_id"]}",exist_ok=True)
        with open(fr"D:\PAGESAVES\GRPumps_pagesave\{category_item["model_id"]}\{model_id}.html", "w") as f:
            f.write(response.text)
        if response.status == 200:
            try:
                known_units = {"mm", "m", "cm", "in", "SSU", "cST", "C", "F", "GPM", "lps", "psi", "PSI", "bar", "BAR","Hz", "kW", "HP", "ft", "%","lb","V","P"}
                model_spec = response.xpath('//table[@id="ProductModelInfoTable"]/tbody/tr[position() >1]')
                model_name = response.xpath('//h2/span[@id="pumpName"]/text()').get().strip()
                model_dict["name"] = model_name
                model_dict["model_id"] = model_id.lower()
                model_dict["secondary_identifiers"] = category_item["secondary_identifiers"]
                model_dict['manufacturer'] = category_item['manufacturer']
                model_dict['description'] = category_item['description']
                model_dict['short_description'] = category_item['short_description']
                model_dict['extra_textual_info'] = "Consult your Gorman-Rupp distributor for detailed information concerning your model. Parts lists can sometimes vary from pump to pump.  Your serial number is critical in determining the correct parts and service requirements for your pump. Information on this page is provided for reference only."


                specification_list = list()
                for spec in model_spec:
                    value_text = spec.xpath('.//td[2]/text()').get() or ""
                    units = [
                        u.replace('"', 'in')
                        for u in dict.fromkeys(re.findall(r'[°]?[A-Za-z%"]+', value_text))
                        if u.replace('"', 'in') in known_units
                    ]

                    specification_list.append({"display_name":spec.xpath('.//td[1]/text()').get(),
                                    "value":spec.xpath('.//td[2]/text()').get(),
                                    "unit": ",".join(units) if units else None
                                    ,"description" : None})

                model_dict["technical_specs"] = specification_list
                model_dict["bom"] = []

            except Exception as e:
                print(str(e))

            #resources
            menual_list1 = []
            attachment_list1 = []
            attachment_ext = ['.dwg','.gif', '.jpg', '.jpeg', '.png', '.mp4', '.mov', '.avi', '.zip', '.stp','.x_t']
            manual_ext = ['.pdf', '.doc', '.docx']

            resource1 = response.xpath('//div[@class="resources sidebar-spacing fade in go"]/h4/following-sibling::div/table/tbody/tr[position()>1]')
            for res in resource1:
                sname = res.xpath('.//td[1]//text()').get()
                description = "".join(res.xpath('.//td[2]//text()').getall())
                if description and description !="":
                    sdescription = self.remove_extra_spaces(description.strip())
                else:
                    sdescription = None
                manual_attach = res.xpath('.//td[3]//a/@href').getall()
                for source in manual_attach:
                    if "https://assets.grpumps.com/" in source:
                        pass
                    else:
                        source = "https://assets.grpumps.com"+source
                    menu_dict = dict()
                    for manual in manual_ext:
                        if source.endswith(manual):
                            menu_dict["name"] = sname
                            menu_dict["description"] = sdescription if sdescription and sdescription!= "" else None
                            menu_dict["manufacturer"] = None
                            menu_dict["publication_date"] = None
                            menu_dict["version"] = None
                            menu_dict["metadata"] = {"collected_at":datetime.now().isoformat(),"data_source_url": self.safe_decode_url(source)}
                            menual_list1.append(menu_dict)
                    for att in attachment_ext:
                        att_dict = dict()
                        if source.endswith(att):
                            att_dict["name"] = sname
                            att_dict["description"] = sdescription if sdescription and sdescription!= "" else None
                            att_dict["file_type"] = att.replace(".", "")
                            att_dict["metadata"] = {"collected_at":datetime.now().isoformat(),"data_source_url": self.safe_decode_url(source)}
                            attachment_list1.append(att_dict)
                    if "Video" in sname:
                        att_dict = dict()
                        att_dict["name"] = sname
                        att_dict["description"] = sdescription if sdescription and sdescription !="" else None
                        att_dict["file_type"] = "video"
                        att_dict["metadata"] = {"collected_at":datetime.now().isoformat(),"data_source_url": self.safe_decode_url(source)}
                        attachment_list1.append(att_dict)
            if menual_list1:
                if category_item['manuals'] and category_item['manuals']!=[]:
                    menual_list1.extend(category_item['manuals'])
                    model_dict["manuals"] = menual_list1
                else:
                    model_dict["manuals"] = menual_list1
            else:
                if category_item['manuals'] and category_item['manuals'] != []:
                    model_dict["manuals"] = category_item['manuals']
                else:
                    model_dict["manuals"] = []

            images_list = response.xpath('//img[contains(@src,"https://assets.grpumps.com")]/@src').getall()
            if images_list:
                images_list.extend(category_item['images'])
            else:
                images_list = category_item["images"]
            model_dict["images"] = images_list
            if attachment_list1:
                if category_item['attachments'] and category_item["attachments"] !=[]:
                    attachment_list1.extend(category_item['attachments'])
                    model_dict["attachments"] = attachment_list1
                else:
                    model_dict["attachments"] = attachment_list1
            else:
                if category_item['attachments'] and category_item["attachments"] !=[]:
                    model_dict['attachments'] = category_item['attachments']
                else:
                    model_dict['attachments'] = []

            model_dict["related_models"] = []
            model_dict['breadcrumbs'] = category_item["breadcrumbs"] + [model_id.lower()]
            model_dict['prices'] = category_item['prices']
            model_dict['warranty_info'] = category_item['warranty_info']
            model_dict['lifecycle_status'] = category_item['lifecycle_status']
            model_dict['release_date'] =None
            model_dict['end_of_life_date'] = category_item['end_of_life_date']
            model_dict['compatibility'] = category_item['compatibility']
            model_dict['certifications'] = category_item['certifications']
            model_dict['market_region'] = category_item['market_region']
            model_dict["metadata"] = {"collected_at":datetime.now().isoformat(),"data_source_url":self.safe_decode_url(response.url),
            "original_languages":[{"language_code":"en",
                                   "language_script_code":"latin",
                                   "region_code":"US"}]}
            # category_item["related_info"].append({f"{model_id}":{
            #                                     "name" : model_name,
            #                                    "images":images_list,
            #                                    "technical_specs":specification_list,
            #                                    "manuals":menual_list1,
            #                                    "attachments":attachment_list1,
            #                                    "metadata":{"collected_at":datetime.now().isoformat(),"data_source_url":self.safe_decode_url(response.url)}
            #                                     }})

            # If this is the last model in the category, insert into DB
            # if len(category_item["related_info"]) == response.meta["total"]:
            #     print(f"✅ Finished all models for: {category_item['name']}")
            self.main_list.append(model_dict)
            self.insert_into_db(model_dict)
            # self.insert_into_db(category_item)

    def closed(self, reason):
        """Called automatically when the spider finishes."""
        output_path = fr"D:\Mansi\Other_Project\GRPumps\gr_pumps_{self.file_date}_new1.json"
        try:
            with open(output_path, "w", encoding="UTF-8") as f:
                json.dump(self.main_list, f, indent=4, ensure_ascii=False)
            print(f"\n✅ Data successfully saved to JSON: {output_path}")
        except Exception as e:
            print(f"\n❌ Error saving JSON: {e}")


if __name__ == "__main__":
    execute("scrapy crawl data_new".split())