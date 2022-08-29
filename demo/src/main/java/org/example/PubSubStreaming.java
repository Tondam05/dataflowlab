package org.example;
import com.google.gson.Gson;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class PubSubStreaming {


    private static final Logger LOG = LoggerFactory.getLogger(PubSubStreaming.class);

    private static TupleTag<TableSchema> ValidMessage = new TupleTag<TableSchema>(){};
    private static TupleTag<String> DlqMessage = new TupleTag<String>(){};

    public interface Options extends DataflowPipelineOptions{
        @Description("Bigquery Table Name")
        String getTableName();
        void setTableName(String tableName);

        @Description("Pubsub Topic")
        String getpubsubTopic();
        void setpubsubTopic(String pubsubTopic);

        @Description("DLQ Topic")
        String getdlqTopic();
        void setdlqTopic(String dlqTopic);
    }


    public static void main(String[] args) {        PipelineOptionsFactory.register(Options.class);
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        run(options);
    }


    static class JSONtoAccountTable extends DoFn<String, TableSchema>{
        @ProcessElement
        public void processElement(@Element String json,ProcessContext processContext)throws Exception{
            Gson gson = new Gson();
            try {
                TableSchema account = gson.fromJson(json, TableSchema.class);
                processContext.output(ValidMessage,account);
            } catch (Exception exception){
                processContext.output(DlqMessage,json);
            }
        }
    }


    public static final Schema rawSchema = Schema
            .builder()
            .addInt32Field("id")
            .addStringField("name")
            .addStringField("surname")
            .build();

    public static PipelineResult run(Options options){
        Pipeline pipeline = Pipeline.create(options);
        PCollectionTuple pubsubMessage = pipeline.apply("Reading message",PubsubIO.readStrings().fromSubscription(options.getpubsubTopic()))
                .apply("Validating it",ParDo.of(new JSONtoAccountTable()).withOutputTags(ValidMessage, TupleTagList.of(DlqMessage)));

        PCollection<TableSchema> ValidData = pubsubMessage.get(ValidMessage);
        PCollection<String> InvalidMessage = pubsubMessage.get(DlqMessage);
        ValidData.apply("Converting",ParDo.of(new DoFn<TableSchema, String>() {
                    @ProcessElement
                    public void convert(ProcessContext context){
                        Gson g = new Gson();
                        String gsonString = g.toJson(context.element());
                        context.output(gsonString);
                    }
                })).apply("Parsing",JsonToRow.withSchema(rawSchema)).
                        apply("inserting msg to bigquery",BigQueryIO.<Row>write().to(options.getTableName())
                        .useBeamSchema()
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
        InvalidMessage.apply("DLQ msg",PubsubIO.writeStrings().to(options.getdlqTopic()));
        return pipeline.run();


    }
}