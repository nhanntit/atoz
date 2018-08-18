require 'mechanize'
require 'cassandra'
require 'watir'
class Importer
  ATTRs = 'bds_id item_id title source link category posted_at expired_at posted_by phone email address product_address price area' \
           ' message_type longitude latitude google_map content front_side back_side front_street floor toilet' \
            ' bed_room living_room furniture paper_status on_project investor project_size direction created_at'
  ATTRs = ATTRs.split

  def self.get_links(short_name)
    links = []

    case short_name
      when '123nd'
        main_link = "http://#{Site::W123ND[:v]}/raovat-c1/nha-dat-ban"
        page = 1
        @browser.goto main_link
        while page < 50
          links += @browser.links(css: 'div.tit_nhadatban h4 a').collect(&:href)
          sleep(rand(0..2))
          page += 1
          @browser.goto main_link + "/#{page}"
        end

      when 'nd24g'
        main_link = "https://#{Site::ND24G[:v]}/ban-bat-dong-san-tphcm-mua-ban-nha-dat-tphcm-s409644"
        page = 1
        @browser.goto main_link
        while page < 100 #500
          links += @browser.links(css: 'div.dv-txt h4 a').collect(&:href)
          sleep(rand(0..2))
          page += 1
          @browser.goto main_link + "/#{page}"
        end

      when 'bds'
        main_link = "https://#{Site::BDS[:v]}/nha-dat-ban"
        page = 1
        @browser.goto main_link
        while page < 100 #to 400
          links += @browser.links(:css, 'div.Main > div > div h3 a').collect(&:href)
          sleep(rand(0..2))
          page += 1
          @browser.goto main_link + "/p#{page}"
        end

      when 'mbnd'
        # main_link = 'http://www.muabannhadat.vn/nha-dat-3490'
        main_link = "http://#{Site::MBND[:v]}/nha-dat-3490/tp-ho-chi-minh-s59"
        page = 1
        @browser.goto main_link
        while page < 100 #1000
          links += @browser.links(:css, 'div.listing-list-img a').collect(&:href)
          sleep(rand(0..2))
          page += 1
          @browser.goto main_link + "?p=#{page}"
        end

      when 'alond'
        main_link = "https://#{Site::ALOND[:v]}/nha-dat/can-ban"
        page = 1
        @browser.goto main_link + '.html'
        while page < 100 #500
          links += @browser.links(:css, 'div.ct_title a').collect(&:href)
          page += 1
          @browser.goto main_link + "/trang--#{page}.html"
          sleep(rand(0..2))
        end
    end
    self.save_links short_name, links.uniq!
    links
  end

  def self.import site, short_name
    @browser =  Watir::Browser.new(:chrome)
    @browser.window.resize_to(1360, 768)
    new_items = []
    existing_links = existing_links site, 1.month.ago
    links = get_available_links short_name
    links = get_links(short_name) if links.empty?
    links =  links - existing_links
    puts "#{links.count} are found and processing."
    Rails.logger.info Time.now.to_s + " #{links.count} links are found and processing."
    count = 0
    until links.empty?
      link =  links.shift
      sleep(rand(0..3))
      begin
        @browser.goto link #rescue next
        attributes = parse_message @browser, short_name
        attributes[:source] = site
        attributes[:link] = link
        attributes[:bds_id] = "#{short_name}_#{attributes[:item_id]}"
        attributes[:created_at] = DateTime.now.utc
        new_items << attributes
      rescue Exception => e
        puts "Error to parse link #{link.to_s} Error message: #{e.message}"
        Rails.logger.info Time.now.to_s + " Error to parse link #{link.to_s} Error message: #{e.message}"
        next
      end
      if new_items.size == 10
        batch_add_items(new_items)
        puts "Added 10 products"
        count += 10
        new_items = []
      end
    end
    batch_add_items(new_items) if new_items.size > 0
    count += new_items.count
    puts "Added #{count} products totally"
    Rails.logger.info Time.now.to_s + "Added #{count} items totally"
    File.delete("tmp/#{short_name}") if File.exist?("tmp/#{short_name}")
  end

  def self.w123nd
    import Site::W123ND[:v], '123nd'
  end

  def self.nd24g
    import Site::ND24G[:v], 'nd24g'
  end

  def self.bds
    import Site::BDS[:v], 'bds'
  end

  def self.alond
    import Site::ALOND[:v], 'alond'
  end

  def self.mbnd
    import Site::MBND[:v], 'mbnd'
  end

  def self.get_available_links short_name
    return [] if !File.exist? "tmp/#{short_name}"
    links = []
    File.open("tmp/#{short_name}").each do |line|
      links << line.to_s
    end
    links.uniq
  end

  def self.save_links site, links
    File.open("tmp/#{site}","a") do |line|
      line.puts links
    end
  end

  def self.parse_message browser, short_name
    case short_name
      when 'alond'
        alond_parse_message(browser)
      when 'bds'
        bds_parse_message(browser)
      when 'mbnd'
        mbnd_parse_message(browser)
      when 'nd24g'
        nd24g_parse_message(browser)
      when '123nd'
        w123nd_parse_message(browser)
    end
  end

  def self.w123nd_parse_message(browser)
    attributes = {}
    info = browser.lis(css: 'div.detail_khungxam > ul > li').collect(&:text)
    info.each do |item|
      value = item.split(':')
      next if value.size != 2
      if value.first.include? 'tầng'
        attributes[:floor] = value.first.delete('tầng').strip
        next
      end  
      if value.first.include? 'phòng ngủ'
        attributes[:bed_room] = value.first.delete('phòng ngủ').strip
        next
      end
      if value.first.include? 'nhà vệ sinh'
        attributes[:toilet] = value.first.delete('nhà vệ sinh').strip
        next
      end

      
      case value.first
      when 'Mã tin'
        attributes[:item_id] = value.second.strip
      when 'Ngày đăng tin'
        attributes[:posted_at] = value.second.strip
      when 'Ngày hết hạn'
        attributes[:expired_at] = value.second.strip
      when 'Giá'
        attributes[:price] = value.second.strip
      when 'Diện tích'
        attributes[:area] = value.second.strip
      when 'Pháp lý'
        attributes[:paper_status] = value.second.strip
      when 'Hướng nhà'
        attributes[:direction] = value.second.strip
      when 'Đường trước nhà'
        attributes[:front_street] = value.second.strip
      end
    end 

    buyer_info = browser.lis(css: 'div.lienhe_nguoiban > ul > li').collect(&:text)
    attributes[:posted_by] = buyer_info.second
    buyer_info.each do |item|
      value = item.split(':')
      next if value.size != 2
      case value.first
      when 'Điện thoại'
        attributes[:phone] = value.second.strip.gsub('.', '')
      when 'Địa chỉ'
        attributes[:address] = value.second.strip
      when 'Email'
        attributes[:email] = value.second.strip
      end

    end
    attributes[:title] = browser.h2(css: 'div.detail_khungxam > div:nth-child(1) > h2').text.strip
    # source: page.css('').text.strip,
    # link: page.css('').text.strip,

    attributes[:category] = browser.select_list(id: 'cboTypeRe').exist? ?
        browser.select_list(id: 'cboTypeRe').text.strip : ''
    attributes[:product_address] = browser.span(css: 'ul.info_no2 > li > span').text.strip
    browser.div(css: 'div.detail_khungxam > div:nth-child(2)').text.strip
    attributes[:message_type] = '' #browser.span(css: 'div.content > div.content_C > div:nth-child(1) > span').text.strip
    attributes[:longitude] = ''
    attributes[:latitude] = ''
    attributes[:google_map] = ''
    attributes[:content] = browser.p(css: 'div.detail_khungxam > p').text.strip.gsub("\n", '')
    
    # attributes[:front_side] = browser
    # attributes[:back_side] = browser
    # attributes[:floor] = browser
    # attributes[:toilet] = browser
    # attributes[:bed_room] = browser
    # attributes[:living_room] = browser
    # attributes[:furniture] = browser
    # attributes[:on_project] = browser
    # attributes[:investor] = browser
    # attributes[:project_size] = browser
    attributes
  end

  def self.nd24g_parse_message(browser)
    attributes = {
        item_id: browser.strong(css: '#content > div > div.ct-in-l > div:nth-child(2) > div > div.dv-m-ct-dt > div.dv-tsbds > div.dv-tb-tsbds > table > tbody > tr:nth-child(3) > td:nth-child(2) > strong').text.strip.to_s,
        title: browser.h1(css: '#content > div > div.ct-in-l > div:nth-child(2) > div > h1').exist? ?
            browser.h1(css: '#content > div > div.ct-in-l > div:nth-child(2) > div > h1').text.strip : '',
        # source: page.css('').text.strip,
        # link: page.css('').text.strip,
        category: browser.link(css: '#ContentPlaceHolder2_lbLoaiBDS > a').exist? ?
            browser.link(css: '#ContentPlaceHolder2_lbLoaiBDS > a').text.strip : '',
        posted_at: self.get_nd24h_date(browser),
        expired_at: '',
        posted_by: browser.link(css: '#ContentPlaceHolder2_viewInfo1_lbHoTen > a').exist? ?
            browser.link(css: '#ContentPlaceHolder2_viewInfo1_lbHoTen > a').text.strip : '',
        phone: browser.link(css: '#viewmobinumber').exist? ?
            browser.link(css: '#viewmobinumber').text.strip.gsub('.', '') : '',
        email: nil,
        address: browser.link(css: '#content > div > div.ct-in-l > div:nth-child(2) > div > div.dv-m-ct-dt > div.dv-slider-dt > div.dv-cont-dt > p > label:nth-child(2) > a').exist? ?
            browser.link(css: '#content > div > div.ct-in-l > div:nth-child(2) > div > div.dv-m-ct-dt > div.dv-slider-dt > div.dv-cont-dt > p > label:nth-child(2) > a').text.strip : '',
        product_address: browser.label(css: '#ContentPlaceHolder2_lbVitri').text.strip + ' ' +
            browser.link(css: '#ContentPlaceHolder2_lbDiaChi > a').text.strip,
        price: browser.label(css: '#ContentPlaceHolder2_lbGiaTien').exist? ?
            browser.label(css: '#ContentPlaceHolder2_lbGiaTien').text.strip : '',
        area: browser.label(css: '#ContentPlaceHolder2_lbDienTich').exist? ?
            browser.label(css: '#ContentPlaceHolder2_lbDienTich').text.strip : '',
        message_type: browser.label(css: '#ContentPlaceHolder2_lbLoaiTin').exist? ?
            browser.label(css: '#ContentPlaceHolder2_lbLoaiTin').text.strip : '',
        longitude: '',
        latitude: '',
        google_map: '',
        content: browser.div(css: '#ContentPlaceHolder2_divContent').text.strip.gsub("\n", ''),
        front_side: '',
        back_side: '',
        front_street: '',
        floor: '',
        toilet: '',
        bed_room: '',
        living_room: '',
        furniture: '',
        paper_status: '',
        on_project: '',
        investor: '',
        project_size: '',
        direction: browser.label(css: '#ContentPlaceHolder2_lbHuong').text.strip
    }

  end

  def self.alond_parse_message(browser)
    attributes = {
        item_id: browser.td(css: '#left > div.property > div.moreinfor1 > div.infor > table > tbody > tr:nth-child(1) > td:nth-child(2)').text.strip.to_s,
        title: browser.h1(css: '#left > div.property > div.title > h1').exist? ?
            browser.h1(css: '#left > div.property > div.title > h1').text.strip : '',
        # source: page.css('').text.strip,
        # link: page.css('').text.strip,
        category: browser.td(css: '#left > div.property > div.moreinfor1 > div.infor > table > tbody > tr:nth-child(3) > td:nth-child(2)').exist? ?
            browser.td(css: '#left > div.property > div.moreinfor1 > div.infor > table > tbody > tr:nth-child(3) > td:nth-child(2)').text.strip : '',
        posted_at: self.get_alond_date(browser),
        expired_at: '',
        posted_by: browser.div(css: '#left > div.property > div.contact > div > div.content > div.name').exist? ?
            browser.div(css: '#left > div.property > div.contact > div > div.content > div.name').text.strip : '',
        phone: browser.div(css: '#left > div.property > div.contact > div > div.content > div.fone').exist? ?
            browser.div(css: '#left > div.property > div.contact > div > div.content > div.fone').text.strip.gsub('.', '') : '',
        email: nil,
        address: browser.span(css: '#MainContent_ctlDetailBox_lblAddressContact').exist? ?
            browser.span(css: '#MainContent_ctlDetailBox_lblAddressContact').text.strip : '',
        product_address: browser.span(css: '#left > div.property > div.address > span.value').exist? ?
            browser.span(css: '#left > div.property > div.address > span.value').text.strip : '',
        price: browser.span(css: '#left > div.property > div.moreinfor > span.price > span.value').exist? ?
            browser.span(css: '#left > div.property > div.moreinfor > span.price > span.value').text.strip : '',
        area: browser.span(css: '#left > div.property > div.moreinfor > span.square > span.value').exist? ?
            browser.span(css: '#left > div.property > div.moreinfor > span.square > span.value').text.strip : '',
        message_type: '',
        longitude: browser.span(css: '#left > div.property > div.image-tab > span.view-map').exist? ?
            browser.span(css: '#left > div.property > div.image-tab > span.view-map').attribute_value('lng') : '',
        latitude: browser.span(css: '#left > div.property > div.image-tab > span.view-map').exist? ?
            browser.span(css: '#left > div.property > div.image-tab > span.view-map').attribute_value('lat') : '',
        google_map: '',
        content: browser.div(css: '#left > div.property > div.detail').text.strip.gsub("\n", ''),
        front_side: browser.td(css: '#left > div.property > div.moreinfor1 > div.infor > table > tbody > tr:nth-child(4) > td:nth-child(2)').text.strip,
        back_side: nil,
        front_street: browser.td(css: '#left > div.property > div.moreinfor1 > div.infor > table > tbody > tr:nth-child(2) > td:nth-child(5)').text.strip,
        floor: browser.td(css: '#left > div.property > div.moreinfor1 > div.infor > table > tbody > tr:nth-child(4) > td:nth-child(4)').text.strip,
        toilet: '',
        bed_room: browser.td(css: '#left > div.property > div.moreinfor1 > div.infor > table > tbody > tr:nth-child(5) > td:nth-child(4)').text.strip,
        living_room: nil,
        furniture: '',
        paper_status: browser.td(css: '#left > div.property > div.moreinfor1 > div.infor > table > tbody > tr:nth-child(3) > td:nth-child(4)').text.strip,
        on_project: '',
        investor: nil,
        project_size: nil,
        direction: browser.td(css: '#left > div.property > div.moreinfor1 > div.infor > table > tbody > tr:nth-child(1) > td:nth-child(4)').text.strip
    }

  end

  def self.get_nd24h_date(browser)
    str = browser.td(css: '#content > div > div.ct-in-l > div:nth-child(2) > div > div.dv-m-ct-dt > div.dv-tsbds > div.dv-tb-tsbds > table > tbody > tr:nth-child(6) > td:nth-child(2)').text.strip
    return DateTime.now.strftime('%d-%m-%Y') if str.include? 'Hôm nay'
    return 1.day.ago.strftime('%d-%m-%Y') if str.include? 'Hôm qua'
    return str.split(',').first.gsub('/', '-')
  end

  def self.get_alond_date(browser)
    str = browser.span(css: '#left > div.property > div.title > span').exist? ? browser.span(css: '#left > div.property > div.title > span').text.strip : ''
    return DateTime.now.strftime('%d-%m-%Y') if str.include? 'Hôm nay'
    return 1.day.ago.strftime('%d-%m-%Y') if str.include? 'Hôm qua'
    return str.gsub('/', '-')
  end  

  def self.mbnd_parse_message(browser)
    attributes = {
        item_id: browser.span(id: 'MainContent_ctlDetailBox_lblId').exist? ? browser.span(id: 'MainContent_ctlDetailBox_lblId').text.strip.to_s : '',
        title: browser.p(css: '#ctl01 > div.body-content > div.jumbotron.head > div > div > div.col-lg-10.col-md-8.hidden-md.hidden-sm.hidden-xs > ol > li.active > p').text.strip,
        # source: page.css('').text.strip,
        # link: page.css('').text.strip,
        category: browser.p(css: '#ctl01 > div.body-content > div.jumbotron.head > div > div > div.col-lg-10.col-md-8.hidden-md.hidden-sm.hidden-xs > ol > li:nth-child(2) > a > p').exist? ?
            browser.p(css: '#ctl01 > div.body-content > div.jumbotron.head > div > div > div.col-lg-10.col-md-8.hidden-md.hidden-sm.hidden-xs > ol > li:nth-child(2) > a > p').text.strip : '',
        posted_at: browser.span(css: '#MainContent_ctlDetailBox_lblDateCreated').exist? ?
            browser.span(css: '#MainContent_ctlDetailBox_lblDateCreated').text.strip.gsub('.', '-') : '',
        expired_at: browser.span(css: '#MainContent_ctlDetailBox_lblDateUpdated').exist? ?
            browser.span(css: '#MainContent_ctlDetailBox_lblDateUpdated').text.strip.gsub('.', '-') : '',
        posted_by: browser.div(css: '#ctl01 > div.body-content > div.container.main.bs-docs-container > div > div > div:nth-child(5) > div.col-md-4.col-xs-12.right-panel > div.row.contact-area > div:nth-child(2) > div > div > div > div > div.col-xs-12.name-contact').exist? ?
            browser.div(css: '#ctl01 > div.body-content > div.container.main.bs-docs-container > div > div > div:nth-child(5) > div.col-md-4.col-xs-12.right-panel > div.row.contact-area > div:nth-child(2) > div > div > div > div > div.col-xs-12.name-contact').text.strip : '',
        phone: self.get_phone(browser),
        email: nil,
        address: browser.span(css: '#MainContent_ctlDetailBox_lblAddressContact').exist? ?
            browser.span(css: '#MainContent_ctlDetailBox_lblAddressContact').text.strip : '',
        product_address: "#{browser.span(css: '#MainContent_ctlDetailBox_lblStreet').exist? ?
            browser.span(css: '#MainContent_ctlDetailBox_lblStreet').text.strip : ''} #{ browser.span(css: '#MainContent_ctlDetailBox_lblWard').a.exist? ?
            browser.span(css: '#MainContent_ctlDetailBox_lblWard').a.text.strip : ''}",
        price: browser.span(css: '#MainContent_ctlDetailBox_lblPrice').exist? ?
            browser.span(css: '#MainContent_ctlDetailBox_lblPrice').text.strip : '',
        area: browser.span(css: '#MainContent_ctlDetailBox_lblSurface').exist? ?
            browser.span(css: '#MainContent_ctlDetailBox_lblSurface').text.strip : '',
        message_type: nil, #page.css('?').text.strip,
        longitude: nil, #page.css('#hdLong').first.attributes['value'].value,
        latitude: nil, #page.css('#hdLat').first.attributes['value'].value,
        google_map: browser.a(css: '#MainContent_ctlDetailBox_lblMapLink > a').exist? ?
            browser.a(css: '#MainContent_ctlDetailBox_lblMapLink > a').href : '',
        content: browser.div(css: '#Description').exist? ? browser.div(css: '#Description').text.strip : '',
        front_side: nil,
        back_side: nil,
        front_street: browser.span(css: '#MainContent_ctlDetailBox_lblFrontRoadWidth').exist? ?
            browser.span(css: '#MainContent_ctlDetailBox_lblFrontRoadWidth').text.strip.gsub("\r\n", " ") : '',
        floor: browser.span(css: '#MainContent_ctlDetailBox_lblFloor').exist? ?
            browser.span(css: '#MainContent_ctlDetailBox_lblFloor').text.strip : '',
        toilet: browser.span(css: '#MainContent_ctlDetailBox_lblBathRoom').exist? ?
            browser.span(css: '#MainContent_ctlDetailBox_lblBathRoom').text.strip : '',
        bed_room: browser.span(css: '#MainContent_ctlDetailBox_lblBedRoom').exist? ?
            browser.span(css: '#MainContent_ctlDetailBox_lblBedRoom').text.strip : '',
        living_room: nil,
        furniture: browser.span(css: '#MainContent_ctlDetailBox_lblUtility').exist? ?
            browser.span(css: '#MainContent_ctlDetailBox_lblUtility').text.strip : '',
        paper_status: browser.span(css: '#MainContent_ctlDetailBox_lblLegalStatus').exist? ?
            browser.span(css: '#MainContent_ctlDetailBox_lblLegalStatus').text.strip : '',
        on_project: browser.span(css: '#MainContent_ctlDetailBox_lblProject').exist? ?
            browser.span(css: '#MainContent_ctlDetailBox_lblProject').text.strip : '',
        investor: nil, #page.css('').text.strip,
        project_size: nil, #page.css('#ContentPlaceHolder_ProductDetail1_projectSize > span.o-value-n').text.strip,
        direction: browser.span(css: '#MainContent_ctlDetailBox_lblFengShuiDirection').exist? ?
            browser.span(css: '#MainContent_ctlDetailBox_lblFengShuiDirection').text.strip : ''
    }

  end

  def self.get_phone(browser)
    return '' if !browser.span(id: 'MainContent_ctlDetailBox_lblContactPhone').exist?
    browser.span(id: 'MainContent_ctlDetailBox_lblContactPhone').link.click
    sleep(2)
    browser.span(id: 'MainContent_ctlDetailBox_lblContactPhone').link.text
  end

  def self.batch_add_items(items)
    cluster =  Cassandra.cluster
    session = cluster.connect('atoz')
    statement =  session.prepare("INSERT INTO bds (#{ATTRs.join(',')}) VALUES (#{('? '*ATTRs.size).split.join(',')})")
    batch = session.batch do |batch|
        items.each do |item|
           batch.add(statement,arguments:
            [
                item[:bds_id],
                item[:item_id],
                item[:title],
                item[:source],
                item[:link],
                item[:category],
                item[:posted_at],
                item[:expired_at],
                item[:posted_by],
                item[:phone],
                item[:email],
                item[:address],
                item[:product_address],
                item[:price],
                item[:area],
                item[:message_type],
                item[:longitude],
                item[:latitude],
                item[:google_map],
                item[:content],
                item[:front_side],
                item[:back_side],
                item[:front_street],
                item[:floor],
                item[:toilet],
                item[:bad_room],
                item[:living_room],
                item[:furniture],
                item[:paper_status],
                item[:on_project],
                item[:investor],
                item[:project_size],
                item[:direction],
                item[:created_at]
            ])
      end
    end
    session.execute(batch)

  end

  def self.bds_parse_message(browser)
    attributes = {
        item_id: browser.div(css: '#product-detail > div.prd-more-info > div:nth-child(1) > div').text.strip.to_s,
        title: browser.h1(css: '#product-detail > div.pm-title > h1').exist? ?
            browser.h1(css: '#product-detail > div.pm-title > h1').text.strip : '',
        # source: page.css('').text.strip,
        # link: page.css('').text.strip,
        category: browser.div(css: '#product-detail > div.div-table > div > div.div-table-cell.table1 > div > div.table-detail > div:nth-child(1) > div.right').exist? ?
            browser.div(css: '#product-detail > div.div-table > div > div.div-table-cell.table1 > div > div.table-detail > div:nth-child(1) > div.right').text.strip : '',
        posted_at: browser.div(css: '#product-detail > div.prd-more-info > div:nth-child(3)').text.split(':').second.strip,
        expired_at: browser.div(css: '#product-detail > div.prd-more-info > div:nth-child(4)').text.split(':').second.strip,
        posted_by: browser.div(css: '#LeftMainContent__productDetail_contactName > div.right').exist? ?
            browser.div(css: '#LeftMainContent__productDetail_contactName > div.right').text.strip : '',
        phone: browser.div(css: '#LeftMainContent__productDetail_contactMobile > div.right').exist? ?
            browser.div(css: '#LeftMainContent__productDetail_contactMobile > div.right').text.strip.gsub('.', '') : '',
        email: browser.div(css: '#contactEmail > div.right').exist? ?
            browser.div(css: '#contactEmail > div.right').text.strip : '',
        address: browser.div(css: '#LeftMainContent__productDetail_contactAddress > div.right').exist? ?
            browser.div(css: '#LeftMainContent__productDetail_contactAddress > div.right').text.strip : '',
        product_address: browser.div(css: '#product-detail > div.div-table > div > div.div-table-cell.table1 > div > div.table-detail > div:nth-child(2) > div.right').exist? ?
            browser.div(css: '#product-detail > div.div-table > div > div.div-table-cell.table1 > div > div.table-detail > div:nth-child(2) > div.right').text.strip : '',
        price: browser.strong(css: 'span.gia-title.mar-right-15 > strong').exist? ?
            browser.strong(css: 'span.gia-title.mar-right-15 > strong').text.strip : '',
        area: browser.strong(xpath: "//*[@id='product-detail']/div[2]/span[2]/span[2]/strong").exist? ?
            browser.strong(xpath: "//*[@id='product-detail']/div[2]/span[2]/span[2]/strong").text.strip : '',
        message_type: browser.div(css: '#product-detail > div.prd-more-info > div:nth-child(2)').text.split(':').second.strip,
        longitude: browser.input(css: '#hdLong').exist? ?
            browser.input(css: '#hdLong').attribute_value('value') : '',
        latitude: browser.input(css: '#hdLat').exist? ?
            browser.input(css: '#hdLat').attribute_value('value') : '',
        google_map: '',
        content: browser.div(css: '#product-detail > div.pm-content > div.pm-desc').text.strip.gsub("\n", ''),
        front_side: browser.div(css: '#LeftMainContent__productDetail_frontEnd > div.right').exist? ?
            browser.div(css: '#LeftMainContent__productDetail_frontEnd > div.right').text.strip : '',
        back_side: '',
        front_street: browser.div(css: '#LeftMainContent__productDetail_wardin > div.right').exist? ?
            browser.div(css: '#LeftMainContent__productDetail_wardin > div.right').text.strip : '',
        floor: browser.div(css: '#LeftMainContent__productDetail_floor > div.right').exist? ?
            browser.div(css: '#LeftMainContent__productDetail_floor > div.right').text.strip : '',
        toilet: browser.div(css: '#LeftMainContent__productDetail_toilet > div.right').exist? ?
            browser.div(css: '#LeftMainContent__productDetail_toilet > div.right').text.strip : '',
        bed_room: browser.div(css: '#LeftMainContent__productDetail_roomNumber > div.right').exist? ?
            browser.div(css: '#LeftMainContent__productDetail_roomNumber > div.right').text.strip : '',
        living_room: '',
        furniture: browser.div(css: '#LeftMainContent__productDetail_interior > div.right').exist? ?
            browser.div(css: '#LeftMainContent__productDetail_interior > div.right').text.strip : '',
        paper_status: '',
        on_project: browser.div(css: '#project > div.table-detail > div:nth-child(1) > div.right').exist? ?
            browser.div(css: '#project > div.table-detail > div:nth-child(1) > div.right').text.strip : '',
        investor: browser.div(css: '#LeftMainContent__productDetail_projectOwner > div.right').exist? ?
            browser.div(css: '#LeftMainContent__productDetail_projectOwner > div.right').text.strip : '',
        project_size: browser.div(css: '#LeftMainContent__productDetail_projectSize > div.right').exist? ?
            browser.div(css: '#LeftMainContent__productDetail_projectSize > div.right').text.strip : '',
        direction: ''
    }

    # attributes = {
    #     item_id: page.css('#form1 > div.body > div > div.slide-body > div.sr-content > div:nth-child(6) > div:nth-child(5) > span.o-value-n').text.strip.to_s,
    #     title: page.css('#form1 > div.body > div > div.slide-body > div.sr-content > div.sr-title > h1').text.strip,
    #     # source: page.css('').text.strip,
    #     # link: page.css('').text.strip,
    #     category: page.css('#form1 > div.body > div > div.slide-body > div.sr-content > div:nth-child(6) > div:nth-child(6) > span.o-value-n').text.strip,
    #     posted_at: page.css('#form1 > div.body > div > div.slide-body > div.sr-content > div:nth-child(6) > div:nth-child(7) > span.o-value-n').text.strip,
    #     expired_at: page.css('#form1 > div.body > div > div.slide-body > div.sr-content > div:nth-child(6) > div:nth-child(8) > span.o-value-n').text.strip,
    #     posted_by: page.css('#ContentPlaceHolder_ProductDetail1_contactName > span.o-value-n').text.strip,
    #     phone: page.css('//*[@id="popup"]/div[2]/div[2]/ul/li/a').text.strip,
    #     email: page.css('#contactEmail > span.o-value-n > a').text.strip,
    #     address: page.css('#ContentPlaceHolder_ProductDetail1_contactAddress > span.o-value-n').text.strip,
    #     product_address: page.css('#form1 > div.body > div > div.slide-body > div.sr-content > div:nth-child(6) > div:nth-child(4) > span.o-value-n').text.strip,
    #     price: page.css('//*[@id="ContentPlaceHolder_ProductDetail1_price"]/span[2]').text.strip,
    #     area: page.css('//*[@id="ContentPlaceHolder_ProductDetail1_Area"]/span[2]').text.strip,
    #     message_type: page.css('#form1 > div.body > div > div.slide-body > div.sr-content > div:nth-child(6) > div:nth-child(9) > span.o-value-n').text.strip,
    #     longitude: page.css('#hdLong').first.attributes['value'].value,
    #     latitude: page.css('#hdLat').first.attributes['value'].value,
    #     google_map: page.css('#hddGmapLibLink').first.attributes['value'].value,
    #     content: page.css('#form1 > div.body > div > div.slide-body > div.sr-content > div:nth-child(3) > span').text.strip,
    #     front_side: page.css('//*[@id="ContentPlaceHolder_ProductDetail1_frontEnd"]/span[2]').text.strip.gsub("\r\n", " "),
    #     back_side: nil,
    #     front_street: page.css('//*[@id="ContentPlaceHolder_ProductDetail1_wardin"]/span[2]').text.strip.gsub("\r\n", " "),
    #     floor: page.css('//*[@id="ContentPlaceHolder_ProductDetail1_floor"]/span[2]').text.strip,
    #     toilet: page.css('//*[@id="ContentPlaceHolder_ProductDetail1_toilet"]/span[2]').text.strip,
    #     bed_room: page.css('//*[@id="ContentPlaceHolder_ProductDetail1_roomNumber"]/span[2]').text.strip,
    #     living_room: nil,
    #     furniture: page.css('//*[@id="ContentPlaceHolder_ProductDetail1_interior"]/span[2]').text.strip,
    #     paper_status: nil,
    #     on_project: page.css('#project > div.o-i-field.nobor > span.o-value-n').text.strip,
    #     investor: page.css('#ContentPlaceHolder_ProductDetail1_projectOwner > span.o-value-n').text.strip,
    #     project_size: page.css('#ContentPlaceHolder_ProductDetail1_projectSize > span.o-value-n').text.strip,
    #     direction: page.css('#ContentPlaceHolder_ProductDetail1_balcony > span.o-value-n').text.strip
    # }

  end

  def self.bds_dummy
    attributes = {
        source: 'bds.com',
        link: 'test/link',
        bds_id: "bds_222",
        item_id: '3434546',
        title: 'ban dat so hong vinh loc a',
        category: 'Dat ban',
        posted_at: '2018-03-12 09:09:10'.to_time,
        expired_at: '2018-03-12 09:09:10'.to_time,
        posted_by: 'Nha Nguyen',
        phone: '0932188189',
        email: 'nhanntit@gmail.com',
        address: '129 Tran Phu',
        product_address: 'Aeon',
        price: '5 ty',
        area: '500 m2',
        message_type: 'test',
        longitude: '23243454465',
        latitude: '23243454466',
        google_map: 'page.css(#hddGmapLibLink).first.attributes[value].value',
        content: "page.css('#form1 > div.body > div.slide-pane > div.sr-content > div:nth-child(3) > span').text.strip",
        front_side: '4',
        back_side: '6',
        front_street: 'sdsd',
        floor: 5,
        toilet: 4,
        bed_room: 8,
        living_room: 7,
        furniture: "dfsdfasd",
        paper_status: "page.css('').text.strip",
        on_project: "page.css('#project > div.o-i-field.nobor > span.o-value-n').text.strip",
        investor: "page.css('#ContentPlaceHolder_ProductDetail1_projectOwner > span.o-value-n').text.strip",
        project_size: "page.css('#ContentPlaceHolder_ProductDetail1_projectSize > span.o-value-n').text.strip",
        direction: 'dong nam'
    }

  end

  def self.existing_links(site, time = 30.day.ago)
    # TODO
    cluster =  Cassandra.cluster
    session = cluster.connect('atoz')
    statement =  session.prepare("SELECT link FROM atoz.bds where source = '" + site + "'")
    # results =  session.execute_async(statement)
    session.execute(statement).rows.pluck("link")
  end

  def self.update_data
    cluster =  Cassandra.cluster
    session = cluster.connect('atoz')
    statement =  session.prepare("SELECT * FROM atoz.bds where source = '123nhadat.vn'")
    # results =  session.execute_async(statement)
    results = session.execute(statement).rows
    results.each_slice(20) do |items|
      batch_update_items items, "phone"#, "alonhadat.com.vn"
    end
  end

  def self.add_test
    agent = Mechanize.new { |a|
      a.post_connect_hooks << lambda { |_,_,response,_|
        if response.content_type.nil? || response.content_type.empty?
          response.content_type = 'text/html'
        end
      }
    }
    link = 'https://m.batdongsan.com.vn/ban-nha-rieng-duong-61-2-phuong-phuoc-long-b/toi-chuyen-cong-tac-iet-thu-9-5-28-9-7-ty-can-nhanh-pr15541327'
    page =  agent.get(link)
    attributes = bds_parse_message(page)
    attributes[:source] = 'bds.com'
    attributes[:link] = link
    attributes[:bds_id] = "bds_#{attributes[:item_id]}"
    attributes[:created_at] = DateTime.now.utc
    batch_add_items([attributes])
  end

  def self.batch_update_items(items, field)
    cluster =  Cassandra.cluster
    session = cluster.connect('atoz')
    statement =  session.prepare("UPDATE bds SET #{field} = ? where bds_id = ?")
    batch = session.batch do |batch|
      items.each do |item|
        # next if item['link'].include? new_value
        batch.add(statement,arguments: [item['phone'].gsub('.', ''), item['bds_id'] ])
      end
    end
    session.execute(batch) if batch.present?
    puts 'updated...'

  end

  def self.new_agent
    Mechanize.new { |a|
      a.post_connect_hooks << lambda { |_,_,response,_|
        if response.content_type.nil? || response.content_type.empty?
          response.content_type = 'text/html'
        end
      }
    }
  end

end
