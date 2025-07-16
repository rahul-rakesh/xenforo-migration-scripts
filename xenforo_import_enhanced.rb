# frozen_string_literal: true

# ---
# Merged XenForo to Discourse Importer (Corrected)
#
# This script combines the performance enhancements of a deferred-execution script
# with the simplicity and immediacy of an in-line processing script.
#
# - High-performance keyset pagination (`WHERE id > last_id`) is used for speed.
# - Avatars, attachments, and permalinks are processed in-line.
# - Includes crucial GC.start calls in all batch loops for memory management.
# - Includes modern features like reaction importing and robust error handling.
# ---

require 'mysql2'
require 'cgi'
require 'timeout'
require 'activerecord-import'
require 'set'

begin
  require "php_serialize"
rescue LoadError
  puts; puts "The 'php_serialize' gem is not installed."
  puts "Please add it to your Gemfile and run 'bundle install'."
  exit
end

require_relative "base.rb"

class ImportScripts::XenForo < ImportScripts::Base
  # --- Configuration ---
  # Set these in your environment or change the defaults here.
  XENFORO_DB = ENV["XF_DB_NAME"] || "xenforo_db"
  TABLE_PREFIX = ENV["XF_TABLE_PREFIX"] || "xf_"
  BATCH_SIZE = ENV["XF_IMPORT_BATCH_SIZE"]&.to_i || 1000
  ATTACHMENT_BATCH_SIZE = ENV["XF_ATTACHMENT_BATCH_SIZE"]&.to_i || 20

  AVATAR_DIR = ENV["XF_AVATAR_DIR"] || "/shared/import_data/data/avatars"
  ATTACHMENT_DIR = ENV["XF_ATTACHMENT_DIR"] || "/shared/import_data/internal_data/attachments"

  SUSPENDED_TILL = Date.new(3000, 1, 1).to_time
  POST_PROCESS_TIMEOUT = 20 # seconds
  STATUS_UPDATE_INTERVAL_SECONDS = 10 # How often to print a status line

  XF_TO_DISCOURSE_EMOJI_MAP = {
    "thumbsup" => "+1", "heart_eyes" => "heart", "rofl" => "rofl",
    "astonished" => "open_mouth", "slight_frown" => "frowning", "rage" => "angry"
  }.freeze

  # --- 2. Define Mappings for Old Data ---
  CONDITION_MAP = {
    "fs_itemcond_01" => "New", "fs_itemcond_02" => "Used - Like New",
    "fs_itemcond_03" => "Used - Good", "fs_itemcond_04" => "Used - Fair",
    "fs_itemcond_05" => "For Parts"
  }.freeze

  SHIPPING_CHARGES_MAP = {
    "fs_shipchar_exclusive" => "Exclusive of shipping charges", "fs_shipchar_inclusive" => "Inclusive of shipping charges",
    "fs_shipchar_actuals"   => "Shipping at actuals", "fs_shipchar_contact"   => "Contact seller for shipping details"
  }.freeze

  PAYMENT_OPTIONS_MAP = {
    "fs_payment_bank" => "Bank Transfer", "fs_payment_cash" => "Cash",
    "fs_payment_paytm" => "PayTM", "fs_payment_gpay" => "Google Pay",
    "fs_payment_phonepe"=> "PhonePe", "fs_payment_upi" => "UPI"
  }.freeze
  def initialize
    super
    puts "Initializing XenForo Importer..."
    db_host = ENV["XF_DB_HOST"] || "localhost"
    db_username = ENV["XF_DB_USERNAME"] || "xenforo"
    db_password = ENV["XF_DB_PASSWORD"] || "abcd1234"
    db_encoding = ENV["XF_DB_ENCODING"] || "utf8mb4"

    begin
      @client = Mysql2::Client.new(host: db_host, username: db_username, password: db_password, database: XENFORO_DB, encoding: db_encoding, symbolize_keys: true, cache_rows: false)
      puts "MySQL client connected successfully."
    rescue Mysql2::Error => e
      puts "!!! FATAL: MySQL Connection Error: #{e.message}"; exit 1
    end

    @node_detail_map = {}
    @last_print_time = Time.now # Initialize the print timer
    @last_print_count = 0 # <-- ADD THIS LINE

    puts "Pre-loading Discourse username map..."
    @discourse_username_map = User.pluck(:id, :username).to_h

    @shutdown_requested = false
  end

  def execute
    puts "Starting import process..."

    import_users
    import_categories
    import_posts
    import_private_messages
    import_reactions
    import_market_feedback
    import_thread_prefixes_as_tags
    #import_attachments
    import_market_threads

    puts "\n\nImport script finished."
    puts "Please rebuild posts with: 'rake posts:remap' and 'rake posts:rebake'"
  end

  #
  # User and Avatar Importing
  #
  def import_users
    puts "\nCreating users (including banned users as suspended)..."
    total_count = mysql_query("SELECT count(*) AS count FROM #{TABLE_PREFIX}user WHERE user_state IN ('valid', 'email_bounce');").first&.dig(:count).to_i
    puts "Found #{total_count} valid users to import."
    return if total_count == 0

    # --- CHECKPOINT LOGIC: READ ---
    # checkpoint_file = "./user_checkpoint.txt"
    last_id = 0
    # if File.exist?(checkpoint_file)
    #   last_id = File.read(checkpoint_file).to_i
    #   puts "Resuming from user_id: #{last_id}"
    # end
    # --- END CHECKPOINT LOGIC ---

    processed_count = last_id
    start_time = Time.now

    loop do
      results = mysql_query("SELECT u.user_id, u.username, u.email, u.custom_title AS title, u.register_date AS created_at, u.last_activity AS last_visit_time, u.is_moderator, u.is_admin, u.is_staff, u.is_banned, ub.ban_date, ub.end_date AS ban_end_date, ub.user_reason AS ban_reason FROM #{TABLE_PREFIX}user u LEFT JOIN #{TABLE_PREFIX}user_ban ub ON u.user_id = ub.user_id AND (ub.end_date = 0 OR ub.end_date > UNIX_TIMESTAMP()) WHERE u.user_id > #{last_id} AND u.user_state IN ('valid', 'email_bounce') ORDER BY u.user_id ASC LIMIT #{BATCH_SIZE};")
      break if results.empty?

      create_users(results, total: total_count, offset: processed_count) do |user|
        next if user[:username].blank?
        data = {
          id: user[:user_id],
          email: user[:email],
          username: user[:username],
          name: user[:username],
          title: user[:title],
          created_at: Time.zone.at(user[:created_at]),
          last_seen_at: Time.zone.at(user[:last_visit_time]),
          moderator: user[:is_moderator].to_i == 1 || user[:is_staff].to_i == 1,
          admin: user[:is_admin].to_i == 1,
          post_create_action: proc { |u| import_avatar(user[:user_id], u) }
        }
        if user[:is_banned].to_i == 1
          data[:active] = false
          data[:suspended_at] = user[:ban_date] ? Time.zone.at(user[:ban_date]) : Time.zone.now
          data[:suspended_till] = user[:ban_end_date].to_i > 0 ? Time.zone.at(user[:ban_end_date]) : SUSPENDED_TILL
          data[:custom_fields] = { import_ban_reason: user[:ban_reason] } if user[:ban_reason].present?
        end
        data
      end
      last_id = results.last[:user_id].to_i
      processed_count += results.size

      #File.write(checkpoint_file, last_id)

      GC.start
      print_status(processed_count, total_count, start_time)
    end
    puts "\nUser import complete."
  end

  def import_avatar(xf_user_id, discourse_user)
    path = File.join(AVATAR_DIR, "l", (xf_user_id / 1000).to_s, "#{xf_user_id}.jpg")
    return unless File.exist?(path)

    upload = create_upload(discourse_user.id, path, "avatar_#{xf_user_id}.jpg")
    if upload&.persisted?
      discourse_user.create_user_avatar
      discourse_user.user_avatar.update(custom_upload_id: upload.id)
      discourse_user.update(uploaded_avatar_id: upload.id)
    end
  rescue => e
    puts "\nError importing avatar for XF ID #{xf_user_id}: #{e.message}"
  end

  #
  # Category Importing
  #
  def import_categories
    puts "\nImporting categories and creating permalinks..."
    all_nodes = mysql_query("SELECT node_id, title, description, parent_node_id, node_name, display_order, lft FROM #{TABLE_PREFIX}node WHERE node_type_id IN ('Category', 'Forum') ORDER BY parent_node_id ASC, lft ASC")
    @node_detail_map = all_nodes.each_with_object({}) { |n, map| map[n[:node_id]] = { node_name: n[:node_name] } }

    create_categories(all_nodes.select { |n| n[:parent_node_id] == 0 }) do |c|
      {
        id: c[:node_id],
        name: CGI.unescapeHTML(c[:title].to_s),
        description: CGI.unescapeHTML(c[:description].to_s),
        position: c[:display_order].to_i,
        post_create_action: proc do |category|
          node_name = @node_detail_map.dig(c[:node_id], :node_name)
          Permalink.find_or_create_by(url: "forums/#{node_name}/", category_id: category.id) if node_name.present?
        end
      }
    end

    create_categories(all_nodes.select { |n| n[:parent_node_id] != 0 }) do |c|
      {
        id: c[:node_id],
        name: CGI.unescapeHTML(c[:title].to_s),
        description: CGI.unescapeHTML(c[:description].to_s),
        position: c[:display_order].to_i,
        parent_category_id: category_id_from_imported_category_id(c[:parent_node_id]),
        post_create_action: proc do |category|
          node_name = @node_detail_map.dig(c[:node_id], :node_name)
          Permalink.find_or_create_by(url: "forums/#{node_name}/", category_id: category.id) if node_name.present?
        end
      }
    end

    puts "Category import complete."
  end

  #
  # Post and Topic Importing
  #
  def import_posts
    puts "\nImporting posts, topics, and creating permalinks using a fast, two-pass method."

    # --- PASS 1 of 2: Importing only TOPICS (first posts) ---
    puts "\n--- PASS 1: Importing topics ---"
    total_topic_count = mysql_query("
      SELECT COUNT(t.first_post_id) AS count
      FROM #{TABLE_PREFIX}thread t
      JOIN #{TABLE_PREFIX}post p ON t.first_post_id = p.post_id
      WHERE t.discussion_state = 'visible' AND p.message_state = 'visible'
    ").first&.dig(:count).to_i

    puts "Found #{total_topic_count} topics to import."
    return if total_topic_count == 0

    # --- Checkpoint Logic for Topics (v2 - More Robust) ---
    topics_checkpoint_file = "./topics_checkpoint.json"
    last_id = 0
    processed_count = 0 # This will track the total count across runs

    if File.exist?(topics_checkpoint_file)
      begin
        checkpoint_data = JSON.parse(File.read(topics_checkpoint_file))
        last_id = checkpoint_data["last_id"].to_i
        processed_count = checkpoint_data["processed_count"].to_i # Resume the total count
        puts "Resuming topic import from post_id: #{last_id} (#{processed_count} topics already imported)."
      rescue JSON::ParserError
        puts "Warning: Could not parse topics checkpoint file. Starting from the beginning."
      end
    end
    # --- End Checkpoint Logic ---

    start_time = Time.now

    loop do
      puts "\nFetching topic batch after ID: #{last_id}"
      results = mysql_query("
        SELECT p.post_id, t.thread_id, t.node_id AS xf_node_id, t.title AS xf_thread_title,
               t.first_post_id, t.view_count, p.user_id, p.message AS xf_post_raw, p.post_date AS created_at
        FROM #{TABLE_PREFIX}post p
        JOIN #{TABLE_PREFIX}thread t ON p.post_id = t.first_post_id
        WHERE p.post_id > #{last_id}
          AND t.discussion_state = 'visible'
          AND p.message_state = 'visible'
        ORDER BY p.post_id ASC
        LIMIT #{BATCH_SIZE}
      ")
      break if results.empty?

      # The offset for create_posts should be the count *before* this batch
      offset_for_batch = processed_count

      create_posts(results, total: total_topic_count, offset: offset_for_batch) do |m|
        discourse_user_id = user_id_from_imported_user_id(m[:user_id]) || Discourse::SYSTEM_USER_ID

        mapped = {
          id: m[:post_id],
          user_id: discourse_user_id,
          created_at: Time.zone.at(m[:created_at]),
          category: category_id_from_imported_category_id(m[:xf_node_id].to_i) || SiteSetting.uncategorized_category_id,
          title: CGI.unescapeHTML(m[:xf_thread_title].to_s),
          views: m[:view_count].to_i,
          post_create_action: proc do |p|
            Permalink.find_or_create_by(url: "threads/#{m[:thread_id]}", topic_id: p.topic_id)
          end
        }

        begin
          mapped[:raw] = process_xenforo_post(m[:xf_post_raw], m[:post_id])
        rescue => e
          puts "\nERROR processing topic content for import_id #{m[:post_id]}. Error: #{e.message}. Importing as plain text."
          mapped[:raw] = m[:xf_post_raw].to_s
        end

        mapped
      end

      last_id = results.last[:post_id].to_i
      processed_count += results.size # Increment the total count

      # Write new checkpoint with both ID and the new total count
      File.write(topics_checkpoint_file, { last_id: last_id, processed_count: processed_count }.to_json)

      GC.start
      print_status(processed_count, total_topic_count, start_time)
    end
    puts "\nTopic import complete."

    # --- PASS 2 of 2: Importing only REPLIES ---
    puts "\n\n--- PASS 2: Importing replies ---"
    total_reply_count = mysql_query("
      SELECT COUNT(p.post_id) AS count
      FROM #{TABLE_PREFIX}post p
      JOIN #{TABLE_PREFIX}thread t ON p.thread_id = t.thread_id
      WHERE p.post_id != t.first_post_id
        AND t.discussion_state = 'visible' AND p.message_state = 'visible'
    ").first&.dig(:count).to_i

    puts "Found #{total_reply_count} replies to import."
    return if total_reply_count == 0

    # --- Checkpoint Logic for Replies (v2 - More Robust) ---
    replies_checkpoint_file = "./replies_checkpoint.json"
    last_id = 0
    processed_count = 0 # Reset for this pass, tracks total replies

    if File.exist?(replies_checkpoint_file)
      begin
        checkpoint_data = JSON.parse(File.read(replies_checkpoint_file))
        last_id = checkpoint_data["last_id"].to_i
        processed_count = checkpoint_data["processed_count"].to_i
        puts "Resuming reply import from post_id: #{last_id} (#{processed_count} replies already imported)."
      rescue JSON::ParserError
        puts "Warning: Could not parse replies checkpoint file. Starting from the beginning."
      end
    end
    # --- End Checkpoint Logic ---

    start_time = Time.now

    loop do
      puts "\nFetching reply batch after ID: #{last_id}"
      results = mysql_query("
        SELECT p.post_id, t.first_post_id, p.user_id, p.message AS xf_post_raw, p.post_date AS created_at
        FROM #{TABLE_PREFIX}post p
        JOIN #{TABLE_PREFIX}thread t ON p.thread_id = t.thread_id
        WHERE p.post_id > #{last_id}
          AND p.post_id != t.first_post_id
          AND t.discussion_state = 'visible'
          AND p.message_state = 'visible'
        ORDER BY p.post_id ASC
        LIMIT #{BATCH_SIZE}
      ")
      break if results.empty?

      successful_imports_in_batch = 0
      offset_for_batch = processed_count

      create_posts(results, total: total_reply_count, offset: offset_for_batch) do |m|
        parent = topic_lookup_from_imported_post_id(m[:first_post_id])

        unless parent && Topic.find_by(id: parent[:topic_id])
          puts "FATAL: Parent topic with first_post_id #{m[:first_post_id]} not found for reply #{m[:post_id]}. Skipping."
          next nil
        end

        discourse_user_id = user_id_from_imported_user_id(m[:user_id]) || Discourse::SYSTEM_USER_ID

        mapped = {
          id: m[:post_id],
          user_id: discourse_user_id,
          created_at: Time.zone.at(m[:created_at]),
          topic_id: parent[:topic_id]
        }

        begin
          mapped[:raw] = process_xenforo_post(m[:xf_post_raw], m[:post_id])
        rescue => e
          puts "\nERROR processing reply content for import_id #{m[:post_id]}. Error: #{e.message}. Importing as plain text."
          mapped[:raw] = m[:xf_post_raw].to_s
        end

        successful_imports_in_batch += 1
        mapped
      end

      last_id = results.last[:post_id].to_i
      processed_count += successful_imports_in_batch

      # Write new checkpoint with both ID and the new total count
      File.write(replies_checkpoint_file, { last_id: last_id, processed_count: processed_count }.to_json)

      GC.start
      print_status(processed_count, total_reply_count, start_time)
    end

    puts "\nReply import complete."
    puts "\nFull post import finished successfully."
  end


  #
  # Private Message Importing
  #
  #
  # Private Message Importing (v3 - Original Logic with Checkpointing)
  #
  def import_private_messages
    puts "\nImporting private messages and creating permalinks..."
    total_count = mysql_query("SELECT COUNT(*) AS count FROM #{TABLE_PREFIX}conversation_message").first&.dig(:count).to_i
    puts "Found #{total_count} PMs to import."; return if total_count == 0

    puts "Pre-loading PM recipient map..."; pm_recipients_map_start_time = Time.now
    pm_recipients_map = Hash.new { |h, k| h[k] = [] }
    mysql_query("SELECT conversation_id, user_id FROM #{TABLE_PREFIX}conversation_recipient WHERE recipient_state = 'active'").each do |r|
      pm_recipients_map[r[:conversation_id]] << r[:user_id]
    end
    puts "PM Recipient map loaded in #{Time.now - pm_recipients_map_start_time}s."

    # --- Robust Checkpoint Logic ---
    checkpoint_file = "./pm_checkpoint.json"
    last_id = 0
    processed_count = 0
    if File.exist?(checkpoint_file)
      begin
        checkpoint_data = JSON.parse(File.read(checkpoint_file))
        last_id = checkpoint_data["last_id"].to_i
        processed_count = checkpoint_data["processed_count"].to_i
        puts "Resuming from PM message_id: #{last_id} (#{processed_count} messages already imported)."
      rescue JSON::ParserError
        puts "Warning: Could not parse PM checkpoint file. Starting from the beginning."
      end
    end
    # --- End Checkpoint Logic ---

    start_time = Time.now

    loop do
      rows = mysql_query("SELECT cm.conversation_id, cm.title AS xf_title, cm.user_id AS xf_starter_id, cm.first_message_id, cmsg.message_id, cmsg.message AS xf_message_raw, cmsg.user_id AS xf_message_user_id, cmsg.message_date AS xf_message_date FROM #{TABLE_PREFIX}conversation_master cm JOIN #{TABLE_PREFIX}conversation_message cmsg ON cmsg.conversation_id = cm.conversation_id WHERE cmsg.message_id > #{last_id} ORDER BY cmsg.message_id ASC LIMIT #{BATCH_SIZE}")
      break if rows.empty?

      offset_for_batch = processed_count # Use the resumed count for accurate progress display
      create_posts(rows, total: total_count, offset: offset_for_batch) do |row|
        mapped = {}
        begin
          Timeout.timeout(POST_PROCESS_TIMEOUT) do
            mapped[:raw] = process_xenforo_post(row[:xf_message_raw], "pm_#{row[:message_id]}")
          end
        rescue Timeout::Error
          puts "\nERROR: PM processing hung for import_id pm_#{row[:message_id]}. Importing as plain text."
          mapped[:raw] = row[:xf_message_raw].to_s.gsub(/\[[^\]]+\]/, ' ')
        end

        mapped[:id] = "pm_#{row[:message_id]}"
        mapped[:user_id] = user_id_from_imported_user_id(row[:xf_message_user_id]) || Discourse::SYSTEM_USER_ID
        mapped[:created_at] = Time.zone.at(row[:xf_message_date].to_i)

        if row[:message_id].to_i == row[:first_message_id].to_i
          participants = (pm_recipients_map[row[:conversation_id]] || []).dup
          participants << row[:xf_starter_id].to_i
          target_ids = participants.uniq.map { |id| user_id_from_imported_user_id(id) }.compact
          next nil if target_ids.size < 1 # Use next nil to skip in create_posts

          mapped[:title] = CGI.unescapeHTML(row[:xf_title].to_s.presence || "Conversation #{row[:conversation_id]}")
          mapped[:archetype] = Archetype.private_message
          mapped[:target_usernames] = target_ids.map { |id| @discourse_username_map[id] }.compact
          mapped[:post_create_action] = proc do |p|
            Permalink.find_or_create_by(url: "conversations/#{row[:conversation_id]}", topic_id: p.topic_id)
          end
        else
          parent = topic_lookup_from_imported_post_id("pm_#{row[:first_message_id]}")
          next nil unless parent # Use next nil to skip in create_posts
          mapped[:topic_id] = parent[:topic_id]
        end
        mapped
      end

      last_id = rows.last[:message_id].to_i
      processed_count += rows.size

      # Write new checkpoint with both ID and the new total count
      File.write(checkpoint_file, { last_id: last_id, processed_count: processed_count }.to_json)

      GC.start
      print_status(processed_count, total_count, start_time)
    end
    puts "\nPrivate message import complete."
  end


  #
  # Reaction Importing (Final, Memory-Efficient, Direct SQL Version)
  #
  #
  # Reaction Importing (Final, Definitive Version)
  #
  #
  # Reaction Importing (Final, Definitive Version)
  #
  def import_reactions
    puts "\nImporting reactions (with Core Like support)..."
    return unless table_exists?("#{TABLE_PREFIX}reaction_content")

    # Step 1: Prepare the reaction mapping (no changes here)
    xf_reactions_map = {}
    if table_exists?("#{TABLE_PREFIX}reaction")
      mysql_query("SELECT reaction_id, emoji_shortname FROM #{TABLE_PREFIX}reaction WHERE active = 1").each do |r|
        shortname = r[:emoji_shortname].to_s.gsub(':', '')
        xf_reactions_map[r[:reaction_id]] = XF_TO_DISCOURSE_EMOJI_MAP[shortname] || shortname
      end
    end
    return if xf_reactions_map.empty?

    total_count = mysql_query("SELECT COUNT(*) AS count FROM #{TABLE_PREFIX}reaction_content WHERE content_type = 'post' AND is_counted = 1").first[:count].to_i
    puts "Found #{total_count} reactions to import."
    return if total_count == 0

    last_id = 0
    processed_count = 0
    start_time = Time.now

    loop do
      batch_sql = "
    SELECT reaction_content_id, content_id, reaction_id, reaction_user_id, reaction_date
    FROM #{TABLE_PREFIX}reaction_content
    WHERE reaction_content_id > #{last_id} AND content_type = 'post' AND is_counted = 1
    ORDER BY reaction_content_id ASC
    LIMIT #{BATCH_SIZE}"
      results = mysql_query(batch_sql)
      break if results.empty?

      # --- NEW: Buffers for both Likes and Custom Reactions ---
      likes_to_create = []
      custom_reactions_buffer = []

      results.each do |row|
        post_id = post_id_from_imported_post_id(row[:content_id])
        user_id = user_id_from_imported_user_id(row[:reaction_user_id])
        reaction_value = xf_reactions_map[row[:reaction_id]]
        created_at = Time.zone.at(row[:reaction_date])

        next unless post_id && user_id && reaction_value

        # --- NEW: Logic to separate Likes from other reactions ---
        if reaction_value == 'heart'
          # This is a core "Like", handle it separately
          likes_to_create << {
            post_id: post_id,
            user_id: user_id,
            post_action_type_id: PostActionType.types[:like],
            created_at: created_at,
            updated_at: created_at
          }
        else
          # This is a custom reaction, use the old buffer method
          custom_reactions_buffer << {
            post_id: post_id,
            user_id: user_id,
            reaction_value: reaction_value,
            created_at: created_at
          }
        end
      end

      # --- NEW: Bulk-insert the core Likes ---
      if likes_to_create.any?
        PostAction.import(
          likes_to_create,
          on_duplicate_key_ignore: { conflict_target: [:post_id, :user_id, :post_action_type_id] },
          validate: false
        )
      end

      # --- This part is for custom reactions only and is mostly unchanged ---
      if custom_reactions_buffer.any?
        parent_reactions_data = {}
        reaction_users_buffer = []

        custom_reactions_buffer.each do |reaction|
          key = [reaction[:post_id], reaction[:reaction_value]]
          if !parent_reactions_data.key?(key) || reaction[:created_at] < parent_reactions_data[key]
            parent_reactions_data[key] = reaction[:created_at]
          end
          reaction_users_buffer << reaction
        end

        parent_records_to_import = parent_reactions_data.map do |key, timestamp|
          { post_id: key[0], reaction_value: key[1], reaction_type: 0, created_at: timestamp, updated_at: timestamp }
        end

        if parent_records_to_import.any?
          DiscourseReactions::Reaction.import(parent_records_to_import, on_duplicate_key_ignore: { conflict_target: [:post_id, :reaction_type, :reaction_value] }, validate: false)
        end

        post_ids_in_batch = parent_reactions_data.keys.map(&:first).uniq
        reaction_id_map = DiscourseReactions::Reaction.where(post_id: post_ids_in_batch).pluck(:post_id, :reaction_value, :id).each_with_object({}) { |(pid, rval, id), h| h[[pid, rval]] = id }

        reaction_user_records_to_import = reaction_users_buffer.map do |user_reaction|
          parent_reaction_id = reaction_id_map[[user_reaction[:post_id], user_reaction[:reaction_value]]]
          next if parent_reaction_id.nil?
          { reaction_id: parent_reaction_id, user_id: user_reaction[:user_id], post_id: user_reaction[:post_id], created_at: user_reaction[:created_at], updated_at: user_reaction[:created_at] }
        end.compact

        if reaction_user_records_to_import.any?
          DiscourseReactions::ReactionUser.import(reaction_user_records_to_import, on_duplicate_key_ignore: { conflict_target: [:reaction_id, :user_id] }, validate: false)
        end
      end

      last_id = results.last[:reaction_content_id].to_i
      processed_count += results.size
      print_status(processed_count, total_count, start_time)
    end

    puts "\nReactions import complete."
    puts "IMPORTANT: You MUST run recount scripts to synchronize all counters."
  end

  # Add this function inside your ImportScripts::XenForo class

  # Add this function inside your ImportScripts::XenForo class

  def import_market_feedback
    puts "\nImporting Market Feedback (Tecenc Trader)..."

    source_table = "#{TABLE_PREFIX}xc_feedback_feedback"
    comments_table = "#{TABLE_PREFIX}xc_feedback_comment"
    thread_table = "#{TABLE_PREFIX}thread"

    unless table_exists?(source_table) && table_exists?(thread_table)
      puts "Source tables for feedback not found. Skipping."
      return
    end

    puts "Pre-loading feedback comments..."
    comments_map = Hash.new { |h, k| h[k] = [] }
    if table_exists?(comments_table)
      mysql_query("SELECT fb_id, message FROM #{comments_table} ORDER BY dateline ASC").each do |c|
        comments_map[c[:fb_id]] << c[:message]
      end
    end
    puts "#{comments_map.keys.size} feedback entries with comments found."

    total_count = mysql_query("SELECT count(*) AS count FROM #{source_table}").first[:count]
    puts "Found #{total_count} feedback records to import."
    return if total_count == 0

    # checkpoint_file = "./market_feedback_checkpoint.txt"
    last_id = 0
    # if File.exist?(checkpoint_file)
    #   last_id = File.read(checkpoint_file).to_i
    #   puts "Resuming from fb_id: #{last_id}"
    # end

    processed_count = 0
    start_time = Time.now

    loop do
      batch_sql = "
      SELECT
        fb.fb_id, fb.foruserid, fb.fromuserid, fb.amount,
        fb.review, fb.threadid, fb.dateline,
        t.user_id AS thread_author_user_id
      FROM #{source_table} AS fb
      JOIN #{thread_table} AS t ON (t.thread_id = fb.threadid)
      WHERE fb.fb_id > #{last_id}
      ORDER BY fb.fb_id ASC
      LIMIT #{BATCH_SIZE}
    "
      #====================================================================
      results = mysql_query(batch_sql)
      break if results.empty?

      feedbacks_to_create = []

      results.each do |row|
        topic_id = Permalink.find_by(url: "threads/#{row[:threadid]}")&.topic_id
        next unless topic_id

        rater_user_id = user_id_from_imported_user_id(row[:fromuserid])
        ratee_user_id = user_id_from_imported_user_id(row[:foruserid])
        seller_user_id = user_id_from_imported_user_id(row[:thread_author_user_id])
        next unless rater_user_id && ratee_user_id && seller_user_id

        rating_type =
          if rater_user_id == seller_user_id
            'seller_gives_buyer'
          else
            'buyer_gives_seller'
          end

        star_rating =
          case row[:amount].to_i
          when 1 then 5
          when 0 then 3
          when -1 then 1
          else next
          end

        ratings = {
          dimension_communication: nil,
          dimension_shipping_timeliness: nil,
          dimension_shipping_packaging: nil,
          dimension_payment_timeliness: nil,
          dimension_overall_experience: nil,
        }
        ratings[:dimension_communication] = star_rating
        ratings[:dimension_overall_experience] = star_rating

        if rating_type == 'buyer_gives_seller'
          ratings[:dimension_shipping_timeliness] = star_rating
          ratings[:dimension_shipping_packaging] = star_rating
        else # seller_gives_buyer
          ratings[:dimension_payment_timeliness] = star_rating
        end

        comments = [row[:review]]
        comments.concat(comments_map[row[:fb_id]]) if comments_map.key?(row[:fb_id])
        full_comment_text = comments.join("\n\n---\n\n").strip

        feedbacks_to_create << ratings.merge(
          topic_id: topic_id,
          rater_user_id: rater_user_id,
          ratee_user_id: ratee_user_id,
          rating_type: rating_type,
          comments: full_comment_text,
          created_at: Time.zone.at(row[:dateline]),
          updated_at: Time.zone.at(row[:dateline])
        )
      end

      if feedbacks_to_create.any?
        TecencTrader::Feedback.import!(
          feedbacks_to_create,
          on_duplicate_key_ignore: {
            conflict_target: [:topic_id, :rater_user_id, :ratee_user_id]
          },
          validate: false
        )
      end

      last_id = results.last[:fb_id].to_i
      processed_count += results.size
      #File.write(checkpoint_file, last_id)

      GC.start
      print_status(processed_count, total_count, start_time)
    end

    puts "\nMarket Feedback import complete."
    puts "Note: User summary stats will NOT be updated yet."
    puts "This requires a rebuild and a subsequent rake task to recalculate them."
  end

  def import_thread_prefixes_as_tags
    puts "\nImporting thread prefixes as tags..."

    prefix_table = "#{TABLE_PREFIX}thread_prefix"
    phrase_table = "#{TABLE_PREFIX}phrase"

    unless table_exists?(prefix_table) && table_exists?(phrase_table)
      puts "Source tables for prefixes or phrases not found. Skipping."
      return
    end

    puts "Loading prefix to tag name mappings from phrase table..."
    prefix_map = {}

    map_sql = "
      SELECT
        pr.prefix_id,
        p.phrase_text AS prefix_title
      FROM #{prefix_table} AS pr
      JOIN #{phrase_table} AS p ON (p.title = CONCAT('thread_prefix.', pr.prefix_id))
      WHERE p.language_id = 0
    "

    mysql_query(map_sql).each do |p|
      prefix_map[p[:prefix_id]] = DiscourseTagging.clean_tag(CGI.unescapeHTML(p[:prefix_title].to_s))
    end

    puts "Found #{prefix_map.size} prefixes to map."

    total_count = mysql_query("SELECT COUNT(*) AS count FROM #{TABLE_PREFIX}thread WHERE prefix_id > 0").first[:count]
    puts "Found #{total_count} threads with prefixes to tag."
    return if prefix_map.empty?

    # --- FIX IS HERE (PART 1) ---
    # Create a guardian object for the system user.
    system_guardian = Guardian.new(Discourse.system_user)

    last_id = 0
    processed_count = 0
    start_time = Time.now

    loop do
      batch_sql = "
      SELECT thread_id, prefix_id
      FROM #{TABLE_PREFIX}thread
      WHERE prefix_id > 0 AND thread_id > #{last_id}
      ORDER BY thread_id ASC
      LIMIT #{BATCH_SIZE}
    "
      results = mysql_query(batch_sql)
      break if results.empty?

      results.each do |row|
        tag_name = prefix_map[row[:prefix_id]]
        next unless tag_name.present?

        topic_id = Permalink.find_by(url: "threads/#{row[:thread_id]}")&.topic_id
        next unless topic_id

        topic = Topic.find_by(id: topic_id)
        next unless topic

        # --- FIX IS HERE (PART 2) ---
        # Use the guardian object we just created.
        DiscourseTagging.tag_topic_by_names(topic, system_guardian, [tag_name], append: true)
      end

      last_id = results.last[:thread_id]
      processed_count += results.size
      print_status(processed_count, total_count, start_time)
    end

    puts "\nThread prefix import complete."
  end

  #
  # Post Content and Attachment Processing
  #
  def process_xenforo_post(raw, import_id)
    return "" if raw.blank?
    s = raw.to_s.dup
    s.scrub!

    s = CGI.unescapeHTML(s)

    # --- FIXES FOR CUSTOM TAGS & FORMATTING ---
    s.gsub!(/(@)?\[USER=\d+\](.+?)\[\/USER\]/i, '@\2')
    s.gsub!(%r{\*\*\s*(\S(?:.|\n)*?\S)\s*\*\*}m, '**\1**')
    s.gsub!(/\[DOUBLEPOST=(\d+)[^\]]*\]\[\/DOUBLEPOST\]/i) do
      timestamp = $1.to_i
      merge_date = Time.at(timestamp).strftime("%b %-d, %Y")
      "\n\n---\n*Post merged on #{merge_date}*\n---\n\n"
    end

    # Table handler
    s.gsub!(/\[TABLE[^\]]*\]\s*([\s\S]+?)\s*\[\/TABLE\]/im) do |table_match|
      table_content = table_match
      table_content.gsub!(/\[td\]\s*([\s\S]*?)\s*\[\/td\]/im) do |cell_match|
        cell_content = $1.to_s
        cell_content.gsub!(/\[b\]([\s\S]*?)\[\/b\]/i, '**\1**')
        if cell_content.include?('•')
          list_items = cell_content.split('•').map(&:strip).reject(&:blank?)
          cell_content = "<ul>" + list_items.map { |item| "<li>#{item}</li>" }.join + "</ul>"
        end
        cell_content.gsub!("\n", "<br>")
        "[td]#{cell_content}[/td]"
      end
      markdown_rows = []
      table_content.scan(/\[tr\]\s*([\s\S]+?)\s*\[\/tr\]/im).each do |row_match|
        row_content = row_match.first
        cells = row_content.scan(/\[td\](.*?)\[\/td\]/im).map { |cell| cell.first.strip }
        next if cells.empty?
        markdown_rows << "| #{cells.join(' | ')} |"
      end
      if markdown_rows.size > 1
        num_columns = markdown_rows.first.count('|') - 1
        if num_columns > 0
          separator = "|#{'---|' * num_columns}"
          markdown_rows.insert(1, separator)
        end
      end
      "\n\n" + markdown_rows.join("\n") + "\n\n"
    end

    # Native Discourse Quote Handler
    s.gsub!(%r{\[quote(?:=([^\]]*))?\]([\s\S]*?)\[/quote\]}im) do |match|
      attribution_string = $1
      content = $2.to_s.strip
      next "" if content.blank?
      username = nil
      source_post_id = nil
      if attribution_string.present?
        post_id_match = attribution_string.match(/, post: (\d+)/)
        if post_id_match
          source_post_id = post_id_match[1].to_i
          username = attribution_string.split(',').first.strip.gsub(/^["']|["']$/, '')
        else
          username = attribution_string.strip.gsub(/^["']|["']$/, '')
        end
      end
      if source_post_id
        topic_mapping = topic_lookup_from_imported_post_id(source_post_id)
        if topic_mapping
          next "[quote=\"#{username}, post:#{topic_mapping[:post_number]}, topic:#{topic_mapping[:topic_id]}\"]\n#{content}\n[/quote]\n\n"
        end
      end
      if username.present?
        "[quote=\"#{username}\"]\n#{content}\n[/quote]\n\n"
      else
        "[quote]\n#{content}\n[/quote]\n\n"
      end
    end

    # --- CORRECTED MEDIA HANDLERS ---
    # Ensures all converted media links are on their own line to enable oneboxing/embedding.
    s.gsub!(/\[EMBED[^\]]*\](.*?)\[\/EMBED\]/i, "\n\n\\1\n\n")
    s.gsub!(%r{\[MEDIA=twitter\](\d+)\[\/MEDIA\]}i, "\n\nhttps://x.com/placeholder/status/\\1\n\n")
    s.gsub!(%r{\[MEDIA=youtube\](.+?)\[\/MEDIA\]}i, "\n\nhttps://youtu.be/\\1\n\n")
    s.gsub!(%r{\[YOUTUBE\](.+?)\[/YOUTUBE\]}i, "\n\nhttps://youtu.be/\\1\n\n")
    # --- END CORRECTED MEDIA HANDLERS ---

    s.gsub!(%r{\[b\]([\s\S]+?)\[/b\]}im, '**\1**')
    s.gsub!(%r{\[i\]([\s\S]+?)\[/i\]}im, '*\1*')
    s.gsub!(%r{\[u\]([\s\S]+?)\[/u\]}im, '[u]\1[/u]')
    s.gsub!(%r{\[s\]([\s\S]+?)\[/s\]}im, '~~s~~\1~~/s~~')
    s.gsub!(%r{\[code.*\]([\s\S]+?)\[/code\]}im) { "```\n#{$1.strip}\n```" }
    s.gsub!(%r{\[url=(?:")?([^"\]]+)(?:")?\]([\s\S]+?)\[/url\]}i, "[\\2](\\1)")
    s.gsub!(%r{\[url\]([\s\S]+?)\[/url\]}i, '\1')
    s.gsub!(%r{\[img\]\s*([^\[\]\s]+?)\s*\[/img\]}i, "![](\\1)")
    s.gsub!(%r{\[list\]([\s\S]*?)\[/list\]}im) { |m| m.gsub(/\[\*\]\s*/, "\n* ").gsub(/\[\/?list\]/, "") }
    s.gsub!(%r{\[list=1\]([\s\S]*?)\[/list\]}im) { |m| m.gsub(/\[\*\]\s*/, "\n1. ").gsub(/\[\/?list=1\]/, "") }
    s.gsub!(%r{\[INDENT\]([\s\S]*?)\[/INDENT\]}im) { |m| "\n> #{$1.to_s.strip.gsub(/\n/, "\n> ")}\n" }

    smileys = {
      ':cool2:' => '😎', ":ohyeah:" => "😄",
      ':no:' => '⛔️', ':cheers:' => ':beer:',
      ':)' => '🙂', ':D' => '😄', ';)' => '😉', ':(' => '🙁', ':P' => '😛', ':p' => '😛', ':bleh:' => '😛',
      ':cool:'=> '😎', ':eek:' => '😮', ':o' => '😮', ':rolleyes:' => '🙄', ':mad:' => '😠', ':confused:' => '😕',
      ':redface:' => '😊', ':oops:' => '😳', ':lol:' => '😂', ':sick:' => '🤢', ':tdown:' => '👎', ':tup:' => '👍',
      ':love:' => '😍', ':wub:' => '🥰', ':ROFL:' => '🤣', '(y)' => '👍', '(n)' => '👎', ":d" => "😄", ":@" => "😠",
      ":S" => "😕", ":s" => "😕", ":happy:" => "😊", ":hap2:" => "😊", ":["=> "🙁", ":gap:" => "😬", ":hug:" => "🤗",
      ":expressionless:" => "😑"
    }
    smileys.sort_by { |k, _v| -k.length }.each { |code, emoji| s.gsub!(Regexp.escape(code), " #{emoji} ") }

    s.gsub!(%r{\[/?(list|url|img|code|b|i|u|s|indent|media|youtube|left|center|right|font|size|color|user)\b[^\]]*\]}i, '')
    s.gsub!(/\n{3,}/, "\n\n")

    s.strip
  end

  # ===================================================================
  # ATTACHMENT IMPORTER (V8 - FINAL TALKATIVE VERSION)
  # ===================================================================

  def import_attachments
    puts "\nImporting attachments (Final, Corrected Method)..."

    attachment_table = "#{TABLE_PREFIX}attachment"
    return unless table_exists?(attachment_table)

    total_count_query = "SELECT COUNT(DISTINCT content_id) as count FROM #{attachment_table} WHERE content_type = 'post'"
    total_count = mysql_query(total_count_query).first[:count].to_i
    puts "Found #{total_count} posts with attachments to process."
    return if total_count == 0

    checkpoint_file = "/shared/tmp/attachment_final_checkpoint.json"
    last_id = 0
    processed_count = 0

    if File.exist?(checkpoint_file)
      begin
        json_data = JSON.parse(File.read(checkpoint_file))
        last_id = json_data["last_id"].to_i
        processed_count = json_data["processed_count"].to_i
        puts "Resuming attachment import from XF post ID: #{last_id} (#{processed_count} posts already processed)."
      rescue JSON::ParserError
        File.delete(checkpoint_file)
        puts "Warning: Could not parse checkpoint file. Deleting it and starting from scratch."
        last_id = 0
        processed_count = 0
      end
    end

    start_time = Time.now
    system_user = Discourse.system_user

    loop do
      post_ids_query = "SELECT DISTINCT content_id FROM #{attachment_table} WHERE content_type = 'post' AND content_id > #{last_id} ORDER BY content_id ASC LIMIT 20"
      batch_post_ids = mysql_query(post_ids_query).map { |r| r[:content_id] }
      break if batch_post_ids.empty?

      puts "\n--- Processing batch of #{batch_post_ids.size} posts (starting after post_id #{last_id}) ---"

      batch_post_ids.each do |xf_post_id|
        begin
          ensure_db_connection!
          @client.ping

          discourse_post_id = post_id_from_imported_post_id(xf_post_id)
          next unless discourse_post_id

          post = Post.find_by(id: discourse_post_id)
          next unless post

          attachments_sql = "SELECT a.attachment_id, a.data_id, d.filename, d.user_id FROM #{TABLE_PREFIX}attachment AS a INNER JOIN #{TABLE_PREFIX}attachment_data AS d ON a.data_id = d.data_id WHERE a.content_id = #{xf_post_id} AND a.content_type = 'post'"
          attachment_details = mysql_query(attachments_sql)

          unless attachment_details.empty?
            uploads_map = {}
            attachment_details.each do |att|
              path_pattern = File.join(ATTACHMENT_DIR, (att[:data_id] / 1000).to_s, "#{att[:data_id]}-*.data")
              found_files = Dir.glob(path_pattern)
              next if found_files.empty?
              owner_id = user_id_from_imported_user_id(att[:user_id]) || system_user.id
              upload = create_upload(owner_id, found_files.first, att[:filename])
              uploads_map[att[:attachment_id]] = upload if upload&.persisted?
            end

            if uploads_map.any?
              new_raw = post.raw.dup
              inline_ids = Set.new

              new_raw.gsub!(/\[ATTACH[^\]]*?\](\d+)\[\/ATTACH\]/i) do |match|
                attach_id = $1.to_i
                inline_ids << attach_id
                uploads_map[attach_id] ? uploads_map[attach_id].to_markdown : ""
              end

              # --- THE FIX IS HERE ---
              # Convert the Set to an Array before subtraction
              attachments_to_append = uploads_map.keys - inline_ids.to_a
              # --- END FIX ---

              if attachments_to_append.any?
                # This logic is from ChatGPT and correctly adds line breaks
                new_raw.gsub!(/<hr>\n<strong>Attachments:<\/strong>.*/m, "")
                new_raw.strip!

                new_raw << "\n\n<hr>\n<strong>Attachments:</strong>\n"

                attachments_to_append.each do |attach_id|
                  new_raw << "\n" << uploads_map[attach_id].to_markdown << "\n"
                end
              end

              if post.raw != new_raw
                post.update_column(:raw, new_raw)
                uploads_map.values.each do |upload|
                  ::UserUpload.find_or_create_by(user_id: post.user_id, upload_id: upload.id)
                end
              end
            end
          end
        rescue => e
          puts "\n[ERROR] on XF post ID #{xf_post_id}: #{e.message}"
        ensure
          last_id = xf_post_id
          processed_count += 1
          File.write(checkpoint_file, { last_id: last_id, processed_count: processed_count }.to_json)
        end
      end

      print_status(processed_count, total_count, start_time)
    end

    puts "\nAttachment import complete."
  end

  # ===================================================================
  # MARKETPLACE THREADS IMPORT FUNCTION (v5.0 - Correct Logic)
  # This version is rewritten to be simpler and more accurate.
  # It directly queries only for threads that are marketplace items
  # and processes them one by one to avoid errors.
  # ===================================================================
  def import_market_threads
    puts "\nImporting market threads (New, Safer Method)..."

    # --- 1. Get a precise count of ONLY the threads we will process ---
    count_sql = "SELECT count(t.thread_id) AS count FROM #{TABLE_PREFIX}thread t JOIN #{TABLE_PREFIX}thread_field_value v ON t.thread_id = v.thread_id WHERE v.field_id = 'fs_item_condition' AND v.field_value IS NOT NULL AND v.field_value != ''"
    total_count = mysql_query(count_sql).first[:count].to_i
    puts "Found #{total_count} threads with an 'fs_item_condition' field to process."
    return if total_count == 0

    # --- 2. Set up checkpointing ---
    checkpoint_file = "./market_threads_checkpoint.txt"
    last_id = 0
    if File.exist?(checkpoint_file)
      last_id = File.read(checkpoint_file).to_i
      puts "Resuming from thread_id: #{last_id}"
    end
    processed_count = 0
    start_time = Time.now
    checkpoint_io = File.open(checkpoint_file, "a")

    # --- 3. Loop in batches through ONLY the valid marketplace threads ---
    loop do
      # This query is now much more specific. It only gets threads that have the required field.
      batch_sql = "SELECT t.thread_id, t.first_post_id, t.post_date, t.last_post_date FROM #{TABLE_PREFIX}thread t JOIN #{TABLE_PREFIX}thread_field_value v ON t.thread_id = v.thread_id WHERE v.field_id = 'fs_item_condition' AND v.field_value IS NOT NULL AND v.field_value != '' AND t.thread_id > #{last_id} ORDER BY t.thread_id ASC LIMIT 500"
      threads_in_batch = mysql_query(batch_sql)
      break if threads_in_batch.empty?

      puts "\n--- Processing batch of #{threads_in_batch.size} threads (starting after thread_id #{last_id}) ---"

      threads_in_batch.each do |thread|
        begin
          # For each valid marketplace thread, NOW we get all its other fields.
          fields_sql = "SELECT field_id, field_value FROM #{TABLE_PREFIX}thread_field_value WHERE thread_id = #{thread[:thread_id]}"
          fields_results = mysql_query(fields_sql)
          fields = fields_results.each_with_object({}) { |row, hash| hash[row[:field_id]] = row[:field_value] }

          # Find the corresponding Discourse topic using its FIRST POST ID.
          topic_lookup = topic_lookup_from_imported_post_id(thread[:first_post_id])
          unless topic_lookup && topic_lookup[:topic_id]
            puts "  -> SKIPPING Thread ID: #{thread[:thread_id]} (Reason: Corresponding Discourse topic not found)."
            next
          end

          discourse_topic = Topic.find_by(id: topic_lookup[:topic_id])
          unless discourse_topic
            puts "  -> SKIPPING Thread ID: #{thread[:thread_id]} (Reason: Topic ID #{topic_lookup[:topic_id]} does not exist)."
            next
          end

          # --- This is the same data-building logic from before ---
          market_data_blob = {}
          extra_post_content = []

          market_data_blob[:price] = fields['fs_expected_price']&.to_i || 0
          market_data_blob[:location] = fields['fs_shipping_from'].presence || "Contact Seller"
          market_data_blob[:condition] = CONDITION_MAP[fields['fs_item_condition']] || "Used - Fair" # We know this exists.
          market_data_blob[:purchase_date] = Date.parse(fields['fs_purchase_date']) rescue (Time.at(thread[:post_date]).to_date - 2.years)
          market_data_blob[:shipping_charges] = SHIPPING_CHARGES_MAP[fields['fs_shipping_charges']] || "Not specified"
          market_data_blob[:warranty] = (Date.strptime(fields['fs_warranty_remaining'], '%d/%m/%Y') > Date.today rescue (fields['fs_warranty_remaining']&.downcase&.include?('yes') || false))
          market_data_blob[:testing_warranty_days] = 0
          market_data_blob[:invoice_status] = fields['fs_invoice_available'] == 'fs_invoice_yes'
          market_data_blob[:shipping_risk] = '50-50'
          market_data_blob[:item_status] = thread[:last_post_date] < 2.months.ago.to_i ? 'Sold' : 'Available'

          # AI Category mapping remains the same
          ai_category_map = @ai_category_map_cache ||= JSON.parse(File.read("market_category_map.json"))
          ai_cats = ai_category_map[thread[:thread_id].to_s] || {}
          market_data_blob[:product_category] = ai_cats["category"] || "General (Non-Tech)"
          market_data_blob[:product_subcategory] = ai_cats["subcategory"] || "Miscellaneous"

          if fields['fs_reason_sale'].present?
            extra_post_content << "### Reason for Sale\n#{fields['fs_reason_sale'].truncate(10000)}"
          end
          if fields['fs_payment_options'].present?
            begin
              options = PHP.unserialize(fields['fs_payment_options'])
              payment_methods = options.keys.map { |k| PAYMENT_OPTIONS_MAP[k] }.compact
              extra_post_content << "### Payment Options\n- #{payment_methods.join("\n- ")}" if payment_methods.any?
            rescue; end
          end
          # --- End of data-building logic ---

          # Create the structured listing record
          listing = TecencMarket::Listing.find_or_initialize_by(topic_id: discourse_topic.id)
          listing.assign_attributes(market_data_blob)
          listing.save!

          # Update the topic custom field
          discourse_topic.custom_fields[::TecencMarket::MARKET_DATA_CUSTOM_FIELD] = market_data_blob
          discourse_topic.save_custom_fields(true)

          # Append to the FIRST post, and only if there's content to add
          if extra_post_content.any?
            first_post = discourse_topic.first_post
            if first_post && !first_post.raw.include?(extra_post_content.first)
              first_post.raw = "#{first_post.raw}\n\n<hr>\n\n#{extra_post_content.join("\n\n")}"
              first_post.save!(validate: false)
            end
          end

          puts "  -> SUCCESS: Processed Topic ID #{discourse_topic.id} (from XF Thread #{thread[:thread_id]})"
          checkpoint_io.puts(thread[:thread_id])
          processed_count += 1

        rescue => e
          puts "\n  -> ERROR on XF Thread ID #{thread[:thread_id]}: #{e.message}"
        end
      end

      last_id = threads_in_batch.last[:thread_id]
      print_status(processed_count, total_count, start_time)
      GC.start
    end

    checkpoint_io.close
    puts "\n\nMarketplace thread import complete."
  end


  #
  # Helper Methods
  #
  # --- ADD THIS ENTIRE METHOD ---
  def ensure_db_connection!
    return if ActiveRecord::Base.connection.active?

    puts "\nWARNING: Database connection appears to be down. Attempting to reconnect..."
    ActiveRecord::Base.connection.reconnect!
    sleep(1) # Pause for a second to let things stabilize
    puts "Connection re-established."
  rescue => e
    puts "\nFATAL: Could not re-establish database connection: #{e.message}"
    # We will exit here because if we can't reconnect, we can't continue.
    exit 1
  end
  # --- END OF METHOD TO ADD ---

  def mysql_query(sql)
    @client.query(sql).to_a
  rescue Mysql2::Error => e
    puts "\n\nMySQL Query Error: #{e.message}"
    puts "SQL: #{sql}"
    []
  end

  def table_exists?(name)
    !mysql_query("SHOW TABLES LIKE '#{Mysql2::Client.escape(name)}'").empty?
  end

  # The entire method is replaced with this new version
  def print_status(current, max, start)
    now = Time.now
    # Only print status if the specified time has passed, or if the import is complete.
    return if max > 0 && current < max && (now - @last_print_time) < STATUS_UPDATE_INTERVAL_SECONDS

    # Calculate window-specific metrics
    window_elapsed = now - @last_print_time
    return if window_elapsed <= 0 # Should not happen with the interval check, but good for safety

    window_count = current - @last_print_count
    current_speed = window_count / window_elapsed

    # Calculate overall progress
    pct = current.to_f / max * 100

    # Use print with a newline to create checkpoints
    print("\nProcessed: %d / %d (%.1f%%) | Current Speed: %.1f records/sec" % [current, max, pct, current_speed])
    $stdout.flush

    # Update state for the next window
    @last_print_time = now
    @last_print_count = current
  end
end

# Run the import
ImportScripts::XenForo.new.perform