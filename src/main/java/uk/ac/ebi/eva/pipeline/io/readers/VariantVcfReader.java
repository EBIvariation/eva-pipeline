package uk.ac.ebi.eva.pipeline.io.readers;

import com.google.common.base.Splitter;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.vcf4.*;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.exceptions.NotAVariantException;
import uk.ac.ebi.eva.commons.core.models.VariantSource;
import uk.ac.ebi.eva.commons.core.models.factories.VariantGenotypedVcfFactory;
import uk.ac.ebi.eva.commons.core.models.factories.VariantVcfFactory;
import uk.ac.ebi.eva.pipeline.runner.exceptions.FileFormatException;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

public class VariantVcfReader
{

    private Vcf4 vcf4;
    private BufferedReader reader;
    private Path path;

    private String filePath;

    private VariantSource source;
    private VariantVcfFactory factory;

    public VariantVcfReader(VariantSource source, String filePath) {
        this(source, filePath, new VariantGenotypedVcfFactory());
    }

    public VariantVcfReader(VariantSource source, String filePath, VariantVcfFactory factory) {
        this.source = source;
        this.filePath = filePath;
        this.factory = factory;
    }

    public boolean open() {
        try {
            path = Paths.get(filePath);
            Files.exists(path);

            vcf4 = new Vcf4();
            if (path.toFile().getName().endsWith(".gz")) {
                this.reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(path.toFile()))));
            } else {
                this.reader = Files.newBufferedReader(path, Charset.defaultCharset());
            }

        }
        catch (IOException  ex) {
            Logger.getLogger(uk.ac.ebi.eva.pipeline.io.readers.VariantVcfReader.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }


        return true;
    }

    public boolean pre() {
        try {
            processHeader();

            // Copy all the read metadata to the VariantSource object
            // TODO May it be that Vcf4 wasn't necessary anymore?
            source.addMetadata("fileformat", vcf4.getFileFormat());
            source.addMetadata("INFO", vcf4.getInfo().values());
            source.addMetadata("FILTER", vcf4.getFilter().values());
            source.addMetadata("FORMAT", vcf4.getFormat().values());
            source.addMetadata("ALT", vcf4.getAlternate().values());
            for (Map.Entry<String, List<String>> otherMeta : vcf4.getMetaInformation().entrySet()) {
                source.addMetadata(otherMeta.getKey(), otherMeta.getValue());
            }
            source.setSamples(vcf4.getSampleNames());

        } catch (Exception ex) {
            Logger.getLogger(uk.ac.ebi.eva.pipeline.io.readers.VariantVcfReader.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }

        return true;
    }

    @Override
    public boolean close() {
        try {
            reader.close();
        } catch (IOException ex) {
            Logger.getLogger(uk.ac.ebi.eva.pipeline.io.readers.VariantVcfReader.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }

    public boolean post() {
        return true;
    }

    public List<Variant> read() {
        String line;
        try {
            while ((line = reader.readLine()) != null && (line.trim().equals("") || line.startsWith("#"))) ;

            Boolean isReference=true;
            List<Variant> variants = null;
            // Look for a non reference position (alternative != '.')
            while (line != null && isReference) {
                try {
                    variants = factory.create(source, line);
                    isReference = false;
                } catch (NotAVariantException e) {  // This line represents a reference position (alternative = '.')
                    line = reader.readLine();
                }
            }
            return variants;

        } catch (IOException ex) {
            Logger.getLogger(org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader.class.getName()).log(Level.SEVERE, null, ex);
        }

        return null;
    }

    public List<Variant> read(int batchSize) {
        List<Variant> listRecords = new ArrayList<>(batchSize);

        int i = 0;
        List<Variant> variants;
        while ((i < batchSize) && (variants = this.read()) != null) {
            listRecords.addAll(variants);
            i += variants.size();
        }

        return listRecords;
    }

    public List<String> getSampleNames() {
        return this.vcf4.getSampleNames();
    }

    public String getHeader() {
        return vcf4.buildHeader().toString();
    }

    private void processHeader() throws IOException, FileFormatException {
        BufferedReader localBufferedReader;

        if (Files.probeContentType(path).contains("gzip")) {
            localBufferedReader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(path.toFile()))));
        } else {
            localBufferedReader = new BufferedReader(new FileReader(path.toFile()));
        }

        boolean header = false;
        String line;

        while ((line = localBufferedReader.readLine()) != null && line.startsWith("#")) {
            if (line.startsWith("##fileformat")) {
                if (line.split("=").length > 1) {
                    vcf4.setFileFormat(line.split("=")[1].trim());
                } else {
                    throw new FileFormatException("");
                }

            } else if (line.startsWith("##INFO")) {
                VcfInfoHeader vcfInfo = new VcfInfoHeader(line);
                vcf4.getInfo().put(vcfInfo.getId(), vcfInfo);

            } else if (line.startsWith("##FILTER")) {
                VcfFilterHeader vcfFilter = new VcfFilterHeader(line);
                vcf4.getFilter().put(vcfFilter.getId(), vcfFilter);

            } else if (line.startsWith("##FORMAT")) {
                VcfFormatHeader vcfFormat = new VcfFormatHeader(line);
                vcf4.getFormat().put(vcfFormat.getId(), vcfFormat);

            } else if (line.startsWith("##ALT")) {
                VcfAlternateHeader vcfAlternateHeader = new VcfAlternateHeader(line);
                vcf4.getAlternate().put(vcfAlternateHeader.getId(), vcfAlternateHeader  );

            } else if (line.startsWith("#CHROM")) {
//               List<String>  headerLine = StringUtils.toList(line.replace("#", ""), "\t");
                List<String> headerLine = Splitter.on("\t").splitToList(line.replace("#", ""));
                vcf4.setHeaderLine(headerLine);
                header = true;

            } else {
                String[] fields = line.replace("#", "").split("=", 2);
                vcf4.addMetaInformation(fields[0], fields[1]);
            }
        }

        if (!header) {
            throw new IOException("VCF lacks a header line (the one starting with \"#CHROM\")");
        }

        localBufferedReader.close();
    }

}

