# frozen_string_literal: true

module Fog
  module Aliyun
    class Storage
      class Real
        # Put details for object
        #
        # ==== Parameters
        # * object_name<~String> - Name of object to look for
        #
        def self.conforming_to_us_ascii!(keys, hash)
          keys.each do |k|
            v = hash[k]
            if !v.encode(::Encoding::US_ASCII, :undef => :replace).eql?(v)
              raise Excon::Errors::BadRequest.new("invalid #{k} header: value must be us-ascii")
            end
          end
        end

        def put_object(bucket_name, object_name, data, options = {})
          bucket = @oss_client.get_bucket(bucket_name)
          return bucket.put_object(object_name) if data.nil?
          # With a single PUT operation you can upload objects up to 5 GB in size.
          if data.size > 5_368_709_120
            bucket.resumable_upload(object_name, data.path)
          end
          bucket.put_object(object_name, :file => data.path)
          # data = Fog::Storage.parse_data(data)
          # puts "dddddddddddd data #{data};;;; headers #{data[:headers]}"
          # headers = data[:headers].merge!(options)
          # self.class.conforming_to_us_ascii! headers.keys.grep(/^x-oss-meta-/), headers
          #
          # puts "dddddddddd2 headers #{headers}"
          # http_options = {
          #     :headers => headers,
          #     :body => data[:body]
          # }
          #
          # resources = {
          #     :bucket => bucket_name,
          #     :object => object_name
          # }
          # @oss_http.put(resources, http_options)
        end

        def put_object_with_body(object, body, options = {})
          bucket_name = options[:bucket]
          bucket_name ||= @aliyun_oss_bucket

          resource = bucket_name + '/' + object
          request(
            expects: [200, 203],
            method: 'PUT',
            path: object,
            bucket: bucket_name,
            resource: resource,
            body: body
          )
        end

        def put_folder(bucket, folder)
          path = folder + '/'
          resource = bucket + '/' + folder + '/'
          request(
            expects: [200, 203],
            method: 'PUT',
            path: path,
            bucket: bucket,
            resource: resource
          )
        end

        def put_multipart_object(bucket, object, file)
          # find the right uploadid
          uploads = list_multipart_uploads(bucket)
          upload = (uploads&.find { |tmpupload| tmpupload['Key'][0] == object })

          uploadedSize = 0
          start_partNumber = 1
          if !upload.nil?
            uploadId = upload['UploadId'][0]
            parts = list_parts(bucket, object, uploadId)
            if !parts.nil? && !parts.empty?
              if parts[-1]['Size'][0].to_i != 5_242_880
                # the part is the last one, if its size is over 5m, then finish this upload
                complete_multipart_upload(bucket, object, uploadId)
                return
              end
              uploadedSize = (parts[0]['Size'][0].to_i * (parts.size - 1)) + parts[-1]['Size'][0].to_i
              start_partNumber = parts[-1]['PartNumber'][0].to_i + 1
            end
          else
            # create upload ID
            uploadId = initiate_multipart_upload(bucket, object)
          end

          if file.size <= uploadedSize
            complete_multipart_upload(bucket, object, uploadId)
            return
          end

          end_partNumber = (file.size + 5_242_880 - 1) / 5_242_880
          file.seek(uploadedSize)

          for i in start_partNumber..end_partNumber
            body = file.read(5_242_880)
            upload_part(bucket, object, i.to_s, uploadId, body)
          end

          complete_multipart_upload(bucket, object, uploadId)
        end

        def initiate_multipart_upload(bucket_name, object_name, options = {})
          path = object + '?uploads'
          resource = bucket_name + '/' + path
          ret = request(
            expects: 200,
            method: 'POST',
            path: path,
            bucket: bucket_name,
            resource: resource
          )
          XmlSimple.xml_in(ret.data[:body])['UploadId'][0]

          # # Using OSS ruby SDK to fix performance issue
          # http_options = {
          #     :headers => options,
          #     :query => {'uploads' => nil}
          # }
          #
          # resources = {
          #     :bucket => bucket_name,
          #     :object => object_name
          # }
          #
          # @oss_http.post(resources, http_options)
        end

        def upload_part(bucket_name, object_name, upload_id, part_number, data, options = {})
          path = object_name + '?partNumber=' + part_number + '&uploadId=' + upload_id
          resource = bucket_name + '/' + path
          request(
            expects: [200, 203],
            method: 'PUT',
            path: path,
            bucket: bucket_name,
            resource: resource,
            body: body
          )
          # Using OSS ruby SDK to fix performance issue
          # data = Fog::Storage.parse_data(data)
          # headers = options
          # headers['Content-Length'] = data[:headers]['Content-Length']
          # http_options = {
          #     :headers => headers,
          #     :query => {'uploadId' => upload_id, 'partNumber' => part_number},
          #     :body => data[:body]
          # }
          #
          # resources = {
          #     :bucket => bucket_name,
          #     :object => object_name
          # }
          # @oss_http.put(resources, http_options)
        end

        def complete_multipart_upload(bucket_name, object_name, uploadId, parts)
          parts = list_parts(bucket_name, object_name, uploadId, options = {})
          request_part = []
          return if parts.empty?
          for i in 0..(parts.size - 1)
            part = parts[i]
            request_part[i] = { 'PartNumber' => part['PartNumber'], 'ETag' => part['ETag'] }
          end
          body = XmlSimple.xml_out({ 'Part' => request_part }, 'RootName' => 'CompleteMultipartUpload')

          path = object_name + '?uploadId=' + uploadId
          resource = bucket_name + '/' + path
          request(
            expects: 200,
            method: 'POST',
            path: path,
            bucket: bucket_name,
            resource: resource,
            body: body
          )
          # data = "<CompleteMultipartUpload>"
          # parts.each_with_index do |part, index|
          #   data << "<Part>"
          #   data << "<PartNumber>#{index + 1}</PartNumber>"
          #   data << "<ETag>#{part}</ETag>"
          #   data << "</Part>"
          # end
          # data << "</CompleteMultipartUpload>"
          #
          # http_options = {
          #     :headers => { 'Content-Length' => data.length },
          #     :query => {'uploadId' => upload_id},
          #     :body => data
          # }
          #
          # resources = {
          #     :bucket => bucket_name,
          #     :object => object_name
          # }
          # @oss_http.post(resources, http_options, nil)
        end
      end
    end
  end
end
