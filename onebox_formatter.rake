# lib/tasks/onebox_formatter.rake

namespace :posts do
  desc "Ensures that all Twitter/X links are on their own lines to enable proper embedding."
  task ensure_onebox_line_breaks: :environment do
    puts "Searching for posts with Twitter/X links to reformat..."

    # This regex will find all twitter.com or x.com status URLs
    twitter_url_regex = %r{(https?:\/\/(?:www\.)?(?:x|twitter)\.com\/\S+\/status(?:es)?\/\d+)}i

    # Find all posts that contain a link to be safe
    posts_to_check = Post.where("raw ILIKE '%twitter.com/%' OR raw ILIKE '%x.com/%'")
    total_posts = posts_to_check.count

    if total_posts == 0
      puts "No posts with Twitter/X links found. Nothing to do."
      return
    end

    puts "Found #{total_posts} posts to check. Beginning formatting..."
    progress = 0

    posts_to_check.find_each do |p|
      progress += 1
      new_raw = p.raw.dup

      # Replace every found Twitter URL with the same URL, but surrounded by newlines
      new_raw.gsub!(twitter_url_regex, "\n\n\\1\n\n")

      # Clean up any excessive newlines that might have been created
      new_raw.gsub!(/\n{3,}/, "\n\n")
      new_raw.strip!

      if new_raw != p.raw
        puts "Formatting post ##{p.id}..."
        p.update_column(:raw, new_raw)
        p.rebake!
      end

      if progress % 200 == 0
        puts "Checked #{progress} / #{total_posts} posts..."
      end
    end

    puts "\n---"
    puts "Formatting complete. All Twitter/X links have been corrected."
  end
end