# # frozen_string_literal: true
# require 'tempfile'
# require 'spec_helper'
# require 'benchmark'
# require 'memory_profiler'
# describe 'Performance tests', :performance => true do
#
#   before(:all) do
#     @conn = Fog::Storage[:aliyun]
#     Fog::Logger.debug('Initializing Aliyun CLI for integration test population...')
#     system("aliyun configure set --language en --region #{@conn.aliyun_region_id} --access-key-id #{@conn.aliyun_accesskey_id} --access-key-secret #{@conn.aliyun_accesskey_secret}")
#   end
#
#   before(:each) do
#     Fog::Logger.debug("Initializing oss bucket for tests: #{@conn.aliyun_oss_bucket}")
#     system("aliyun oss rm --bucket oss://#{@conn.aliyun_oss_bucket} -r -f > /dev/null || exit 0")
#     system("aliyun oss mb oss://#{@conn.aliyun_oss_bucket} > /dev/null")
#   end
#
#   it 'Should upload 5 mb file' do
#     content = "A" * 5 * 1024 * 1024
#     directory = @conn.directories.get(@conn.aliyun_oss_bucket)
#     Benchmark.bm(9)  do |benchmark|
#       benchmark.report('Upload file time:') do
#         @memReport = MemoryProfiler.report do
#           directory.files.create :key => 'test_dir/test_file1',
#                             :body => content
#           end
#       end
#     end
#     @memReport.pretty_print :detailed_report => true, :scale_bytes => true
#   end
#
#   it 'Should download 5 mb file' do
#     content = "A" * 5 * 1024 * 1024
#     directory = @conn.directories.get(@conn.aliyun_oss_bucket)
#     directory.files.create :key => 'test_dir/test_file1',
#                            :body => content
#     Benchmark.bm(9)  do |benchmark|
#       benchmark.report('Download file time:') do
#         @memReport = MemoryProfiler.report do
#           files = directory.files
#           file = files.get('test_dir/test_file1')
#           file.body
#         end
#       end
#     end
#     @memReport.pretty_print :detailed_report => true, :scale_bytes => true
#   end
#
# end