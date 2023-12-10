# frozen_string_literal: true

require 'edn'
require 'semantic'

# rubocop:disable Metrics/ClassLength
class Version
  # rubocop:disable Metrics/MethodLength
  def self.from_file(path, options = {})
    parts = if File.exist?(path)
              File.open(path) { |file| EDN.read(file) }
            else
              { major: 0, minor: 0, patch: 0 }
            end

    major = parts[:major]
    minor = parts[:minor]
    patch = parts[:patch]
    pre_release = parts[:'pre-release']
    build = parts[:build]

    build_part = (build ? "+#{build}" : '').to_s
    pre_release_part = (pre_release ? "-#{pre_release}" : '').to_s
    string = "#{major}.#{minor}.#{patch}#{pre_release_part}#{build_part}"

    Version.new(string, options)
  end
  # rubocop:enable Metrics/MethodLength

  def initialize(version_string, options = {})
    @version = Semantic::Version.new(version_string)
    @options = options
  end

  def major
    @version.major
  end

  def minor
    @version.minor
  end

  def patch
    @version.patch
  end

  def pre
    @version.pre
  end

  def build
    @version.build
  end

  def bump(type)
    case type
    when :pre
      bump_pre
    when :patch, :minor, :major
      bump_component(type)
    else
      self
    end
  end

  def prerelease
    bump(:pre)
  end

  def release
    version = @version.clone
    version.pre = nil

    Version.new(version.to_s, @options)
  end

  def to_s
    @version.to_s
  end

  private

  def read_option(path, default)
    branch = path[0...-1]
    leaf = path[-1]
    map = branch.inject(@options) { |acc, step| acc[step] || {} }
    map[leaf] || default
  end

  def pre_descriptor
    read_option(%i[pre descriptor], 'RC')
  end

  def pre_separator
    read_option(%i[pre descriptor], '')
  end

  def pre_prefix
    pre_descriptor + pre_separator
  end

  def pre_numeric?
    read_option(%i[pre number?], true)
  end

  def pre_bumps
    read_option(%i[pre bumps], :minor)
  end

  # rubocop:disable Metrics/AbcSize
  # rubocop:disable Metrics/MethodLength
  def bump_pre
    return self unless pre_numeric?

    version = @version.clone

    if version.pre.nil?
      version = version.increment!(pre_bumps)
      version.pre = "#{pre_prefix}1"

      return Version.new(version.to_s, @options)
    end

    if version.pre =~ /#{pre_prefix}\d+/
      current_pre_number = version.pre.delete_prefix(pre_prefix).to_i
      next_pre_number = current_pre_number + 1
      next_pre = "#{pre_prefix}#{next_pre_number}"
      version.pre = next_pre

      return Version.new(version.to_s, @options)
    end

    Version.new(version.to_s, @options)
  end
  # rubocop:enable Metrics/AbcSize
  # rubocop:enable Metrics/MethodLength

  def bump_component(type)
    version = @version.clone
    version = version.increment!(type)

    Version.new(version.to_s, @options)
  end
end
# rubocop:enable Metrics/ClassLength
