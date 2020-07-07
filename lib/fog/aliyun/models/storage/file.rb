# frozen_string_literal: true

require 'fog/core/model'

module Fog
  module Aliyun
    class Storage
      class File < Fog::Model
        identity :key, aliases: ['Key', 'Name', 'name']
        # attr_writer :body
        attribute :date, aliases: 'Date'
        attribute :content_length, aliases: 'Content-Length', type: :integer
        attribute :content_type, aliases: 'Content-Type'
        attribute :connection, aliases: 'Connection'
        attribute :content_disposition, aliases: 'Content-Disposition'
        attribute :etag, aliases: 'Etag'
        attribute :last_modified, aliases: 'Last-Modified', type: :time
        attribute :accept_ranges, aliases: 'Accept-Ranges'
        attribute :server, aliases: 'Server'
        attribute :object_type, aliases: ['x-oss-object-type', 'x_oss_object_type']

        # @note Chunk size to use for multipart uploads.
        #     Use small chunk sizes to minimize memory. E.g. 5242880 = 5mb
        # attr_reader :multipart_chunk_size
        # def multipart_chunk_size=(mp_chunk_size)
        #   raise ArgumentError.new("minimum multipart_chunk_size is 5242880") if mp_chunk_size < 5242880
        #   @multipart_chunk_size = mp_chunk_size
        # end

        def body
          attributes[:body] ||=
            if last_modified
              collection.get(identity).body
            else
              ''
            end
        end

        def body=(new_body)
          attributes[:body] = new_body
        end

        attr_reader :directory

        def copy(target_directory_key, target_file_key, options = {})
          requires :directory, :key
          source_bucket, directory_key = collection.check_directory_key(directory.key)
          source_object = if directory_key == ''
                            key
                          else
                            directory_key + '/' + key
                          end
          target_bucket, target_directory_key = collection.check_directory_key(target_directory_key)
          target_object = if target_directory_key == ''
                            target_file_key
                          else
                            target_directory_key + '/' + target_file_key
                          end
          service.copy_object(source_bucket, source_object, target_bucket, target_object, options)
          target_directory = service.directories.new(key: target_directory_key)
          target_directory.files.get(target_file_key)
        end

        def destroy
          requires :directory, :key
          bucket_name, directory_key = collection.check_directory_key(directory.key)
          object = if directory_key == ''
                     key
                   else
                     directory_key + '/' + key
                   end
          service.delete_object(object, bucket: bucket_name)
          true
        end

        def metadata
          attributes[:metadata] ||= headers_to_metadata
        end

        def owner=(new_owner)
          if new_owner
            attributes[:owner] = {
              display_name: new_owner['DisplayName'],
              id: new_owner['ID']
            }
          end
        end

        def public=(new_public)
          new_public
        end

        # Get a url for file.
        #
        #     required attributes: directory, key
        #
        # @param expires [String] number of seconds (since 1970-01-01 00:00) before url expires
        # @param options [Hash]
        # @return [String] url
        #
        def url(expires, options = {})

          expires = expires.nil? ? 0 : expires.to_i

          requires :directory, :key
          bucket_name, directory_key = collection.check_directory_key(directory.key)
          object = if directory_key == ''
                     key
                   else
                     directory_key + '/' + key
                   end
          service.get_object_http_url_public(object, expires, options.merge(bucket: bucket_name))
        end

        def public_url
          requires :key
          collection.get_url(key)
        end

        def save(options = {})
          requires :body, :directory, :key
          options['Content-Type'] = content_type if content_type
          options['Content-Disposition'] = content_disposition if content_disposition
          options.merge!(metadata_to_headers)
          bucket_name, directory_key = collection.check_directory_key(directory.key)
          object = if directory_key == ''
                     key
                   else
                     directory_key + '/' + key
                   end
          if body.is_a?(::File)
            service.put_object(bucket_name, object, body, options)
          elsif body.is_a?(String)
            service.put_object_with_body(object, body, options.merge(bucket: bucket_name))
          else
            raise Fog::Aliyun::Storage::Error, " Forbidden: Invalid body type: #{body.class}!"
          end

          begin
            data = service.head_object(object, bucket: bucket_name)
            update_attributes_from(data)
            refresh_metadata

            self.content_length = Fog::Storage.get_body_size(body)
            self.content_type ||= Fog::Storage.get_content_type(body)
            true
          rescue Exception => error
            case error.http_code.to_i
              when 404
                nil
              else
                raise(error)
            end
          end
        end

        # def save(options = {})
        #   requires :body, :directory, :key
        #   options['Content-Type'] = content_type if content_type
        #   options['Content-Disposition'] = content_disposition if content_disposition
        #   options.merge!(metadata_to_headers)
        #   # bucket_name, directory_key = collection.check_directory_key(directory.key)
        #   # object = if directory_key == ''
        #   #            key
        #   #          else
        #   #            directory_key + '/' + key
        #   #          end
        #
        #   # With a single PUT operation you can upload objects up to 5 GB in size. Automatically set MP for larger objects.
        #   self.multipart_chunk_size = 5242880 if !multipart_chunk_size && Fog::Storage.get_body_size(body) > 5368709120
        #   puts "\n\n ******* body size #{Fog::Storage.get_body_size(body)}\n\n;;size #{body.size};  body #{body}"
        #   if multipart_chunk_size && Fog::Storage.get_body_size(body) >= multipart_chunk_size && body.respond_to?(:read)
        #
        #     data = multipart_save(options)
        #     merge_attributes(data.body)
        #   else
        #     puts "\n\n ******* directory key #{directory.key}; class #{directory.key.class};; format_directory_key(directory.key) #{format_directory_key(directory.key)};;;;; key #{key}\n\n"
        #     data = service.put_object(format_directory_key(directory.key), key, body, options)
        #     puts "\n\n ******* data headers #{data.headers}\n\n"
        #     # merge_attributes(data.headers.reject {|key, value| [:content_length, :content_type].include?(key)})
        #   end
        #   # body.respond_to?(:read)
        #   # self.etag = self.etag.gsub('"','') if self.etag
        #   # # puts "\n\n ******* body #{body};;;;; data.body #{data.body}\n\n"
        #   # self.content_length = Fog::Storage.get_body_size(body)
        #   # self.content_type ||= Fog::Storage.get_content_type(body)
        #   # true
        #
        #   begin
        #     data = service.head_object(key, bucket: format_directory_key(directory.key))
        #     puts "\n\n head object: data: #{data}; headers #{data.headers};s\n\n body ize #{body.size}"
        #     self.content_length = data.headers[:content_length]
        #     self.content_type ||= data.headers[:content_type]
        #     puts "\n\n return true \n\n"
        #     true
        #   rescue Exception => error
        #     case error.http_code.to_i
        #       when 404
        #         nil
        #       else
        #         raise(error)
        #     end
        #   end
        #   # if body.is_a?(::File)
        #   #   service.put_object(object, body, options.merge(bucket: bucket_name))
        #   # elsif body.is_a?(String)
        #   #   service.put_object_with_body(object, body, options.merge(bucket: bucket_name))
        #   # else
        #   #   raise Fog::Aliyun::Storage::Error, " Forbidden: Invalid body type: #{body.class}!"
        #   # end
        #   #
        #   # begin
        #   #   data = service.head_object(key, bucket: format_directory_key(directory.key))
        #   #   update_attributes_from(data)
        #   #   refresh_metadata
        #   #
        #   #   self.content_length = Fog::Storage.get_body_size(body)
        #   #   self.content_type ||= Fog::Storage.get_content_type(body)
        #   #   true
        #   # rescue Exception => error
        #   #   case error.http_code.to_i
        #   #     when 404
        #   #       nil
        #   #     else
        #   #       raise(error)
        #   #   end
        #   # end
        # end

        private

        attr_writer :directory
        # def directory=(new_directory)
        #   @directory = new_directory
        # end

        # def multipart_save(options)
        #   # Initiate the upload
        #   begin
        #     res = service.initiate_multipart_upload(directory.key, key, options)
        #     upload_id = res.body["UploadId"]
        #   rescue Exception => error
        #     raise(error)
        #   end
        #
        #   # Store ETags of upload parts
        #   part_tags = []
        #
        #   # Upload each part
        #   # TODO: optionally upload chunks in parallel using threads
        #   # (may cause network performance problems with many small chunks)
        #   # TODO: Support large chunk sizes without reading the chunk into memory
        #   if body.respond_to?(:rewind)
        #     body.rewind  rescue nil
        #   end
        #   while (chunk = body.read(multipart_chunk_size)) do
        #     # TODO: Support encryption_headers
        #     part_upload = service.upload_part(directory.key, key, upload_id, part_tags.size + 1, chunk, options)
        #     part_tags << part_upload.headers[:etag]
        #   end
        #
        #   if part_tags.empty? #it is an error to have a multipart upload with no parts
        #     # TODO: Support encryption_headers
        #     part_upload = service.upload_part(directory.key, key, upload_id, 1, '', options)
        #     part_tags << part_upload.headers[:etag]
        #   end
        #
        # rescue
        #   # Abort the upload & reraise
        #   service.abort_multipart_upload(directory.key, key, upload_id) if upload_id
        #   raise
        # else
        #   # Complete the upload
        #   service.complete_multipart_upload(directory.key, key, upload_id, part_tags)
        # end

        def refresh_metadata
          metadata.reject! { |_k, v| v.nil? }
        end

        def headers_to_metadata
          key_map = key_mapping
          Hash[metadata_attributes.map { |k, v| [key_map[k], v] }]
        end

        def key_mapping
          key_map = metadata_attributes
          key_map.each_pair { |k, _v| key_map[k] = header_to_key(k) }
        end

        def header_to_key(opt)
          opt.gsub(metadata_prefix, '').split('-').map { |k| k[0, 1].downcase + k[1..-1] }.join('_').to_sym
        end

        def metadata_to_headers
          header_map = header_mapping
          Hash[metadata.map { |k, v| [header_map[k], v] }]
        end

        def header_mapping
          header_map = metadata.dup
          header_map.each_pair { |k, _v| header_map[k] = key_to_header(k) }
        end

        def key_to_header(key)
          metadata_prefix + key.to_s.split(/[-_]/).map(&:capitalize).join('-')
        end

        def metadata_attributes
          if last_modified
            bucket_name, directory_key = collection.check_directory_key(directory.key)
            object = if directory_key == ''
                       key
                     else
                       directory_key + '/' + key
                     end


            begin
              data = service.head_object(object, bucket: bucket_name)
              if data.code.to_i == 200
                headers = data.headers
                headers.select! { |k, _v| metadata_attribute?(k) }
              end
            rescue Exception => error
              case error.http_code.to_i
                when 404
                  {}
                else
                  raise(error)
              end
            end
          else
            {}
          end
        end

        def metadata_attribute?(key)
          key.to_s =~ /^#{metadata_prefix}/
        end

        def metadata_prefix
          'x_oss_meta_'
        end

        def update_attributes_from(data)
          merge_attributes(data.headers.reject { |key, _value| [:content_length, :content_type].include?(key) })
        end

        # def format_directory_key(key)
        #   if !key.nil? && (key.is_a? Array) && (key.size > 0)
        #     key[0]
        #   end
        # end
      end
    end
  end
end
