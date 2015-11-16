/*
 * FILE: hrtimer_x86.cc
 * DESCRIPTION: A high-resolution timer on x86 architecture.
 * Last modified date: Nov 29, 2012
 *
 * Changes: Modified the method of getting the CPU frequency from Linux filesystem in getMHZ_x86()
 * and added code to read /sys/devices/system/cpu/cpu0/cpufreq/scaling_max_freq which returns 
 * the maximum operating frequency of CPU when dynamic frequency scaling is enabled in the system.

 * Reasons to modify: Previous implementation of hrtimer_x86.cc, parsed the Linux - /proc/cpuinfo 
 * to get the CPU frequency but if dynamic frequency scaling in enabled, the frequency listed in 
 * /proc/cpuinfo can vary depending on the current operating frequency. This might create discrepencies 
 * in getting the elapsed time via gethrtime_x86 (since gethrtime_x86 calls getMHZ_x86() which returns 
 * the number of CPU cycles per microseconds).
 * 
 * Assumption: The assumption in the previous implementation also holds in this implementation which 
 * considers the system to be composed of homogeneous multicores i.e. all the cores have same operating 
 * frequency.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hrtimer_x86.h"

/* get the elapsed time (in seconds) since startup */
double gethrtime_x86(void)
{
    static double CPU_MHZ=0;
    if (CPU_MHZ==0) CPU_MHZ=getMHZ_x86();
    return (gethrcycle_x86()*0.000001)/CPU_MHZ;
}

/* get the number of CPU cycles since startup */
hrtime_t gethrcycle_x86(void)
{
    unsigned int tmp[2];

    __asm__ ("rdtsc"
	     : "=a" (tmp[1]), "=d" (tmp[0])
	     : "c" (0x10) );
        
    return ( ((hrtime_t)tmp[0] << 32 | tmp[1]) );
}

/* get the number of CPU cycles per microsecond - from Linux /proc filesystem 
 * return<0 on error
 */
double getMHZ_x86(void)
{
    double mhz = -1;
    char line[1024], *s, search_str[] = "cpu MHz";
    FILE *fp; 
    
    /* open /sys/devices/system/cpu/cpu0/cpufreq/scaling_max_freq and check if frequency scaling is enabled */
    if ((fp = fopen("/sys/devices/system/cpu/cpu0/cpufreq/scaling_max_freq", "r")) == NULL || fgets(line, 1024, fp) == NULL)
    {
	if (fp!=NULL) fclose(fp);
	
	/* open proc/cpuinfo */
        if ((fp = fopen("/proc/cpuinfo", "r")) == NULL)
		return -1;

    	/* ignore all lines until we reach MHz information */
   	 while (fgets(line, 1024, fp) != NULL) { 
		if (strstr(line, search_str) != NULL) {
	    	/* ignore all characters in line up to : */
	    	for (s = line; *s && (*s != ':'); ++s);
	    	/* get MHz number */
	    	if (*s && (sscanf(s+1, "%lf", &mhz) == 1)) 
			break;
		}
    	}

    	if (fp!=NULL) fclose(fp);
    }
    /* frequency scaling is enabled, so read /sys/devices/system/cpu/cpu0/cpufreq/scaling_max_freq */
    else
    {
    	/* read the maximum frequency Hz and convert it to MHz*/
	mhz = atof(line)/1000.0;

	if (fp!=NULL) fclose(fp);    
    }

    return mhz;
}
