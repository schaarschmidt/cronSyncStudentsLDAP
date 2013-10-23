
require 'thor'
require 'ruby-plsql'
require 'redis'
require 'net-ldap'
require 'awesome_print'
require 'digest/sha1'
require 'json'
require 'pry'
require 'active_support/core_ext'

class SyncStudentsLDAP < Thor

  LDAP_CHECKSUM_SET = 'students:s_checksum_ldap_students'
  LDAP_UID_SET      = 'students:s_uid_ldap_students'
  UMT_CHECKSUM_SET  = 'students:s_checksum_umt'
  UMT_UID_SET       = 'students:s_uid_umt'
  UMT_ACCOUNT_BY_UID_HASH  = 'students:h_accounts_by_uid'
  UMT_ACCOUNT_BY_CHECKSUM_HASH  = 'students:h_accounts_by_checksum'

  desc 'new','add all missing accounts to the ldap'
  def new
    cleanup_redis_db
    fetch_umt
    fetch_ldap

    counter = 0
    missing_entries.each do |uid|
      counter += 1
      puts "#{counter}: #{uid}"
      write_new_entry uid
    end
  end

  desc "update","update ldap accounts"
  def update
    cleanup_redis_db
    fetch_umt
    fetch_ldap

    unless 0 == missing_entries.size
      puts "[ERROR] there are missing entries left."
      puts "[ERROR] run 'sync_students_ldap new' first"
      exit
    end

    update_candidates.each do |checksum|
      write_update_entry checksum
    end
  end

private
  def connect_redis
    @redis ||= Redis.new
  end

  def connect_umt
    plsql.connection = OCI8.new(
      ENV.fetch('UMT_USER'),
      ENV.fetch('UMT_PASSWORD'),
      ENV.fetch('UMT_SID'))
  end

  def connect_ldap
    unless @ldap
      @ldap = Net::LDAP.new
      @ldap.host = ENV.fetch('LDAP2_ITZ_HOST')
      @ldap.port = ENV.fetch('LDAP2_ITZ_PORT')
#      @ldap.encryption :simple_tls
      @ldap.auth ENV.fetch('LDAP2_ITZ_USER'), ENV.fetch('LDAP2_ITZ_PASSWORD')
    end
  end

  def cleanup_redis_db
    connect_redis
    @redis.del LDAP_CHECKSUM_SET
    @redis.del LDAP_UID_SET
    @redis.del UMT_CHECKSUM_SET
    @redis.del UMT_UID_SET
    @redis.del UMT_ACCOUNT_BY_CHECKSUM_HASH
    @redis.del UMT_ACCOUNT_BY_UID_HASH
  end

  def fetch_umt
    connect_umt
    connect_redis

    records = nil
    plsql.students_pkg.studentsList { |cursor| records = cursor.fetch_all }

    records.each do |record|
      checksum = build_checksum record

      entry = {
        firstname: record[0],
        lastname:  record[1],
        date_of_birth: record[2],
        gender:      record[3],
        matrikel: record[4],
        mlustudstatus: record[5],
        mlusemester: record[6],
        chipseriennr: record[7],
        mlustg1abschl:   record[8],
        mlustg1f1:       record[9],
        mlustg1f1fachkz: record[10],
        mlustg1f1hs:     record[11],
        mlustg1f1pvers:  record[12],
        mlustg1f1sem:    record[13],
        mlustg1f2:       record[14],
        mlustg1f2fachkz: record[15],
        mlustg1f2hs:     record[16],
        mlustg1f2pvers:  record[17],
        mlustg1f2sem:    record[18],
        mlustg1f3:       record[19],
        mlustg1f3fachkz: record[20],
        mlustg1f3hs:     record[21],
        mlustg1f3pvers:  record[22],
        mlustg1f3sem:    record[23],
        mlustg1f4:       record[24],
        mlustg1f4fachkz: record[25],
        mlustg1f4hs:     record[26],
        mlustg1f4pvers:  record[27],
        mlustg1f4sem:    record[28],
        mlustg2abschl:   record[29],
        mlustg2f1:       record[30],
        mlustg2f1fachkz: record[31],
        mlustg2f1hs:     record[32],
        mlustg2f1pvers:  record[33],
        mlustg2f1sem:    record[34],
        mlustg2f2:       record[35],
        mlustg2f2fachkz: record[36],
        mlustg2f2hs:     record[37],
        mlustg2f2pvers:  record[38],
        mlustg2f2sem:    record[39],
        mlustg2f3:       record[40],
        mlustg2f3fachkz: record[41],
        mlustg2f3hs:     record[42],
        mlustg2f3pvers:  record[43],
        mlustg2f3sem:    record[44],
        mlustg2f4:       record[45],
        mlustg2f4fachkz: record[46],
        mlustg2f4hs:     record[47],
        mlustg2f4pvers:  record[48],
        mlustg2f4sem:    record[49],
        nkz:             record[50],
        mail:            record[51],
        checksum:        checksum}

      @redis.hmset(
        UMT_ACCOUNT_BY_CHECKSUM_HASH,
        checksum,
        entry.to_json)

      @redis.hmset(
        UMT_ACCOUNT_BY_UID_HASH,
        entry[:nkz],
        entry.to_json)

      @redis.sadd UMT_CHECKSUM_SET, checksum
      @redis.sadd UMT_UID_SET, entry[:nkz]
    end

  end

  def fetch_ldap
    connect_ldap
    connect_redis

    filter = Net::LDAP::Filter.eq 'uid','*'
    basedn = 'ou=students,o=mlu,c=de'
    attr = ['carLicense','uid']


    @ldap.search(base: basedn, filter: filter, attributes: attr) do |entry|
      unless entry[:carLicense].empty?
        @redis.sadd LDAP_CHECKSUM_SET,entry[:carLicense][0]
      end
      @redis.sadd LDAP_UID_SET,entry[:uid][0]
    end
  end

  def write_new_entry uid
    connect_ldap
    connect_redis
    entry = JSON.parse(
      @redis.hget UMT_ACCOUNT_BY_UID_HASH, uid).
      symbolize_keys

    dn = "uid=#{entry[:nkz]},ou=students,o=mlu,c=de"
    attributes = {
      uid: entry[:nkz],
      sn: entry[:lastname],
      givenname: entry[:firstname],
      cn: "#{entry[:firstname]} #{entry[:lastname]}",
      carlicense: "#{entry[:checksum]}",
      mail: "#{entry[:mail]}",
      mlustudstatus: "#{entry[:mlustudstatus]}",
      schacgender: "#{entry[:gender]}",
      mlugebdat: "#{entry[:date_of_birth]}",
      mlumatrikel: "#{entry[:matrikel]}",
      mlupersontype: "2",
      mlusemester: "#{entry[:mlusemester]}",
      mlustatus: "active",
      mlustg1abschl: "#{entry[:mlustg1abschl]}",
      mlustg1f1: "#{entry[:mlustg1f1]}",
      mlustg1f1hs: "#{entry[:mlustg1f1hs]}",
      mlustg1f1pvers: "#{entry[:mlustg1f1pvers]}",
      mlustg1f1fachkz: "#{entry[:mlustg1f1fachkz]}",
      mlustg1f1sem: "#{entry[:mlustg1f1sem]}",
      mlustg1f2: "#{entry[:mlustg1f2]}",
      mlustg1f2hs: "#{entry[:mlustg1f2hs]}",
      mlustg1f2pvers: "#{entry[:mlustg1f2pvers]}",
      mlustg1f2fachkz: "#{entry[:mlustg1f2fachkz]}",
      mlustg1f2sem: "#{entry[:mlustg1f2sem]}",
      mlustg1f3: "#{entry[:mlustg1f3]}",
      mlustg1f3hs: "#{entry[:mlustg1f3hs]}",
      mlustg1f3pvers: "#{entry[:mlustg1f3pvers]}",
      mlustg1f3fachkz: "#{entry[:mlustg1f3fachkz]}",
      mlustg1f3sem: "#{entry[:mlustg1f3sem]}",
      mlustg1f4: "#{entry[:mlustg1f4]}",
      mlustg1f4hs: "#{entry[:mlustg1f4hs]}",
      mlustg1f4pvers: "#{entry[:mlustg1f4pvers]}",
      mlustg1f4fachkz: "#{entry[:mlustg1f4fachkz]}",
      mlustg1f4sem: "#{entry[:mlustg1f4sem]}",
      mlustg2abschl: "#{entry[:mlustg2abschl]}",
      mlustg2f1: "#{entry[:mlustg2f1]}",
      mlustg2f1hs: "#{entry[:mlustg2f1hs]}",
      mlustg2f1pvers: "#{entry[:mlustg2f1pvers]}",
      mlustg2f1fachkz: "#{entry[:mlustg2f1fachkz]}",
      mlustg2f1sem: "#{entry[:mlustg2f1sem]}",
      mlustg2f2: "#{entry[:mlustg2f2]}",
      mlustg2f2hs: "#{entry[:mlustg2f2hs]}",
      mlustg2f2pvers: "#{entry[:mlustg2f2pvers]}",
      mlustg2f2fachkz: "#{entry[:mlustg2f2fachkz]}",
      mlustg2f2sem: "#{entry[:mlustg2f2sem]}",
      mlustg2f3: "#{entry[:mlustg2f3]}",
      mlustg2f3hs: "#{entry[:mlustg2f3hs]}",
      mlustg2f3pvers: "#{entry[:mlustg2f3pvers]}",
      mlustg2f3fachkz: "#{entry[:mlustg2f3fachkz]}",
      mlustg2f3sem: "#{entry[:mlustg2f3sem]}",
      mlustg2f4: "#{entry[:mlustg2f4]}",
      mlustg2f4hs: "#{entry[:mlustg2f4hs]}",
      mlustg2f4pvers: "#{entry[:mlustg2f4pvers]}",
      mlustg2f4fachkz: "#{entry[:mlustg2f4fachkz]}",
      mlustg2f4sem: "#{entry[:mlustg2f4sem]}",
      objectClass: [
        "top",
        "person",
        "organizationalPerson",
        "inetOrgPerson",
        "mluPerson",
        "mluStudent",
        "schacPersonalCharacteristics"]}

    unless @ldap.add dn: dn, attributes: attributes.select{ |k,v| !v.empty? }
      puts "Result: #{@ldap.get_operation_result.code}"
      puts "Message: #{@ldap.get_operation_result.message}"
    end

    puts "Eintrag geschrieben: #{entry[:nkz]}"
  end

  def get_account_by_checksum checksum
    JSON.parse(
      @redis.hget UMT_ACCOUNT_BY_CHECKSUM_HASH, checksum).
      symbolize_keys
  end

  def write_update_entry checksum
    entry = get_account_by_checksum checksum

    puts "Eintrag geholt: #{entry[:nkz]}"

    dn = "uid=#{entry[:nkz]},ou=students,o=mlu,c=de"

    unless @ldap.delete dn: dn
      puts "Result: #{@ldap.get_operation_result.code}"
      puts "Message: #{@ldap.get_operation_result.message}"
    end

    write_new_entry entry[:nkz]
  end

  def missing_entries
    @redis.sdiff UMT_UID_SET, LDAP_UID_SET
  end

  def update_candidates
    @redis.sdiff UMT_CHECKSUM_SET, LDAP_CHECKSUM_SET
  end

  def build_checksum array
    Digest::SHA1.hexdigest array.inject('') {|string,item| string + item.to_s}
  end
end

SyncStudentsLDAP.start
