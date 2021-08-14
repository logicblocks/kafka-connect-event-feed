require 'edn'
require 'semantic'

class Version
  def self.from_file(path)
    parts =
      File.exist?(path) ?
        File.open(path) { |file| EDN.read(file) } :
        {
          major: 0,
          minor: 0,
          patch: 0
        }

    major = parts[:major]
    minor = parts[:minor]
    patch = parts[:patch]
    pre_release = parts[:'pre-release']
    build = parts[:build]

    string = major.to_s + '.' + minor.to_s + '.' + patch.to_s +
      (pre_release ? "-#{pre_release}" : '') +
      (build ? "+#{build}" : '')

    Version.new(string)
  end

  def initialize(version_string)
    @version = Semantic::Version.new(version_string)
  end

  def to_s
    @version.to_s
  end
end
