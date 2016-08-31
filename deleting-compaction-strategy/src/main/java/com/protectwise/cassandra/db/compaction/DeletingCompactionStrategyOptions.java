/*
 * Copyright 2015 ProtectWise, Inc.  All rights reserved
 * Unauthorized copying of this file by any means is strictly prohibited.
 */
package com.protectwise.cassandra.db.compaction;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public final class DeletingCompactionStrategyOptions
{
    private static final Logger logger = LoggerFactory.getLogger(DeletingCompactionStrategy.class);

    protected static final String CONVICTOR_CLASSNAME_KEY = "dcs_convictor";
    protected static final String UNDERLYING_CLASSNAME_KEY = "dcs_underlying_compactor";
    protected static final String DRY_RUN_KEY = "dcs_is_dry_run";
    protected static final String DELETED_RECORDS_DIRECTORY = "dcs_backup_dir";
    protected static final String STATUS_REPORT_INTERVAL = "dcs_status_report_ms";

    protected final String convictorClassName;
    protected final Map<String, String> convictorOptions;
    protected final AbstractCompactionStrategy underlying;
    protected final ColumnFamilyStore cfs;
    protected final boolean enabled;
    protected final boolean dryRun;
    protected final File deletedRecordsSinkDirectory;
    protected final long statsReportInterval;

    @SuppressWarnings("unchecked")
    public DeletingCompactionStrategyOptions(ColumnFamilyStore cfs, Map<String, String> options)
    {
        this.cfs = cfs;
        boolean enabled = true;


        convictorClassName = options.get(CONVICTOR_CLASSNAME_KEY);
        this.convictorOptions = options;

        String optionValue = options.get(UNDERLYING_CLASSNAME_KEY);
        // TODO: See if there is a unified means in cassandra-core for resolving short names;
        // found snippets like this in other areas, but it seems like there should be something
        // more general purpose or reusable.
        if (!optionValue.contains("."))
        {
            optionValue = "org.apache.cassandra.db.compaction." + optionValue;
        }
        AbstractCompactionStrategy underlying;
        try
        {
            Class<AbstractCompactionStrategy> underlyingClass = FBUtilities.classForName(optionValue, "deleting compaction underlying compactor");

            Constructor<AbstractCompactionStrategy> constructor = underlyingClass.getConstructor(ColumnFamilyStore.class, Map.class);
            underlying = constructor.newInstance(cfs, options);

        }
        catch (ConfigurationException|NoSuchMethodException|InstantiationException|IllegalAccessException|InvocationTargetException e)
        {
            logger.error(String.format(
                    "Unable to instantiate underlying compactor class %s: %s",
                    optionValue,
                    e.getMessage()
            ), e);
            underlying = null;
            enabled = false;
        }
        this.underlying = underlying;

        boolean dryRun;
        if (options.containsKey(DRY_RUN_KEY))
        {
            optionValue = options.get(DRY_RUN_KEY);
            // if we get _anything_ unexpected, default to being a dry run.
            dryRun = Boolean.parseBoolean(optionValue);
        } else {
            dryRun = false;
        }

        File backupDir = null;
        if (options.containsKey(DELETED_RECORDS_DIRECTORY))
        {
            try
            {
                backupDir = validateBackupDirectory(new File(options.get(DELETED_RECORDS_DIRECTORY)));
            }
            catch (ConfigurationException e)
            {
                dryRun = true;
                logger.warn("Deletion backup directory cannot be used.  Compaction will revert to dry run.", e);
            }
        }

        long statusReportInterval = 0l;
        if (options.containsKey(STATUS_REPORT_INTERVAL))
        {
            statusReportInterval = Long.parseLong(options.get(STATUS_REPORT_INTERVAL));
        }

        this.deletedRecordsSinkDirectory = backupDir;
        this.dryRun = dryRun;
        this.enabled = enabled;
        this.statsReportInterval = statusReportInterval;
    }

    public AbstractSimpleDeletingConvictor buildConvictor() {
        AbstractSimpleDeletingConvictor convictor;
        try
        {
            Class<AbstractSimpleDeletingConvictor> convictorClass = FBUtilities.classForName(convictorClassName, "deleting compaction convictor");
            Constructor constructor = convictorClass.getConstructor(ColumnFamilyStore.class, Map.class);
            convictor = (AbstractSimpleDeletingConvictor)constructor.newInstance(cfs, convictorOptions);
        }
        catch (ConfigurationException|NoSuchMethodException|IllegalAccessException|InvocationTargetException|InstantiationException e)
        {
            logger.error(String.format(
                    "Unable to instantiate convictor class %s: %s",
                    convictorClassName,
                    e.getMessage()
            ), e);
            convictor = null;
        }
        return convictor;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        String optionValue = options.get(CONVICTOR_CLASSNAME_KEY);
        Class<AbstractSimpleDeletingConvictor> convictor = FBUtilities.classForName(optionValue, "deleting compaction convictor");
        if (!AbstractSimpleDeletingConvictor.class.isAssignableFrom(convictor))
        {
            throw new ConfigurationException(String.format(
                    "%s must implement %s to be used as a deleting compaction strategy convictorClass",
                    convictor.getCanonicalName(),
                    AbstractSimpleDeletingConvictor.class.getCanonicalName()
            ));
        }
        options.remove(CONVICTOR_CLASSNAME_KEY);

        optionValue = options.get(UNDERLYING_CLASSNAME_KEY);
        Class<AbstractCompactionStrategy> underlyingClass = FBUtilities.classForName(optionValue, "deleting compaction underlying compactor");
        if (!AbstractCompactionStrategy.class.isAssignableFrom(underlyingClass))
        {
            throw new ConfigurationException(String.format(
                    "%s must implement %s to be used as a deleting compaction strategy underlying compactor",
                    underlyingClass.getCanonicalName(),
                    AbstractCompactionStrategy.class.getCanonicalName()
            ));
        }
        options.remove(UNDERLYING_CLASSNAME_KEY);

        if (options.containsKey(DRY_RUN_KEY))
        {
            optionValue = options.get(DRY_RUN_KEY);
            if (!optionValue.equals("true") && !optionValue.equals("false")) {
                throw new ConfigurationException(String.format(
                        "%s must either be 'true' or 'false' - received '%s'",
                        DRY_RUN_KEY,
                        optionValue
                ));
            }
            options.remove(DRY_RUN_KEY);
        }

        if (options.containsKey(DELETED_RECORDS_DIRECTORY))
        {
            optionValue = options.get(DELETED_RECORDS_DIRECTORY);
            // Although these conditions can change after the strategy is applied to a table, or may not even be
            // consistent across the entire cluster, it doesn't hurt to validate that at least at the time it's set up,
            // initial conditions on the coordinating host look good.
            validateBackupDirectory(new File(optionValue));
            options.remove(DELETED_RECORDS_DIRECTORY);
        }

        if (options.containsKey(STATUS_REPORT_INTERVAL))
        {
            optionValue = options.get(STATUS_REPORT_INTERVAL);
            Long.parseLong(optionValue);
            options.remove(STATUS_REPORT_INTERVAL);
        }

        return validatePassthrough(convictor, validatePassthrough(underlyingClass, options));
    }

    protected static File validateBackupDirectory(File dir) throws ConfigurationException
    {
        // Do some basic santiy checks here to see if it seems likely we can do delete backups

        // If the directory doesn't exist, attempt to create it.
        if (!dir.exists())
        {
            if (!dir.mkdirs()) {
                throw new ConfigurationException("The directory " + dir.getAbsolutePath() + " does not exist, and could not be created automatically.");
            }
        }
        if (!dir.isDirectory())
        {
            throw new ConfigurationException("The path " + dir.getAbsolutePath() + " is not a directory, it cannot be used for data backups.");
        }

        if (!dir.canWrite())
        {
            throw new ConfigurationException("The directory " + dir.getAbsolutePath() + " cannot be written to, it cannot be used for data backups.");
        }

        return dir;
    }

    @SuppressWarnings("unchecked")
    protected static Map<String, String> validatePassthrough(Class<?> convictor, Map<String, String> options) throws ConfigurationException
    {
        try
        {
            java.lang.reflect.Method subValidate = convictor.getMethod("validateOptions", Map.class);
            options = (Map<String, String>)subValidate.invoke(null, options);
        }
        catch (NoSuchMethodException | IllegalAccessException e)
        {
            throw new ConfigurationException(String.format(
                    "Convictor (%s) options validation failed: %s %s",
                    convictor.getCanonicalName(),
                    e.getClass().getSimpleName(),
                    e.getMessage()
            ));
        }
        catch (InvocationTargetException e)
        {
            throw new ConfigurationException(String.format(
                    "Convictor (%s) options validation failed: %s %s\n%s %s",
                    convictor.getCanonicalName(),
                    e.getClass().getSimpleName(),
                    e.getMessage(),
                    e.getCause().getClass().getSimpleName(),
                    e.getCause().getMessage()
            ));

        }

        return options;
    }

    public boolean isEnabled()
    {
        return enabled;
    }
}
