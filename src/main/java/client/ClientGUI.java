/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package client;

import javax.swing.*;
import java.awt.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author gound
 */
public class ClientGUI extends JFrame {

    private JComboBox<String> jobType;
    private JTextField inputField;
    private JButton submitButton;
    private JButton loadCsvButton;
    private JTextArea resultArea;

    public ClientGUI() {

        setTitle("CS324 Distributed System - Client");
        setSize(600, 400);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLocationRelativeTo(null);

        // Main panel
        JPanel mainPanel = new JPanel(new BorderLayout(10, 10));

        mainPanel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15)
        );

        
        // Job selection
        JPanel topPanel = new JPanel(new FlowLayout());

        JLabel jobLabel = new JLabel("Select Job:");

        jobType = new JComboBox<>(new String[]{
            "MAX",
            "PRIMESUM",
            "PRIMECOUNT"
        });

        topPanel.add(jobLabel);
        topPanel.add(jobType);

        
        // Input section
        JPanel inputPanel = new JPanel(new BorderLayout(5, 5));

        JLabel inputLabel = new JLabel("Input:");

        inputField = new JTextField();

        inputPanel.add(inputLabel, BorderLayout.WEST);
        inputPanel.add(inputField, BorderLayout.CENTER);

        
        // Buttons
        JPanel buttonPanel = new JPanel(new FlowLayout());

        loadCsvButton = new JButton("Load CSV");
        submitButton = new JButton("Submit Job");

        buttonPanel.add(loadCsvButton);
        buttonPanel.add(submitButton);

        // Result area
        resultArea = new JTextArea();

        resultArea.setEditable(false);

        JScrollPane scrollPane = new JScrollPane(resultArea);

        // Add components
        mainPanel.add(topPanel,BorderLayout.NORTH);

        mainPanel.add(inputPanel,BorderLayout.CENTER);

        mainPanel.add(buttonPanel,BorderLayout.SOUTH);

        add(mainPanel,BorderLayout.NORTH);

        add(scrollPane,BorderLayout.CENTER);

        // Button actions
        submitButton.addActionListener(
                e -> submitJob()
        );

        loadCsvButton.addActionListener(
                e -> loadCsvFile()
        );
    }

    
    //Loads numbers from a CSV file.
    private void loadCsvFile() {

        JFileChooser fileChooser = new JFileChooser();

        int result = fileChooser.showOpenDialog(this);

        if (result != JFileChooser.APPROVE_OPTION) {
            return;
        }

        File file = fileChooser.getSelectedFile();

        try {

            List<String> numbers = new ArrayList<>();

            BufferedReader reader = new BufferedReader(
                         new FileReader(file)
                    );

            String line;

            while ((line = reader.readLine()) != null) {

                String[] values = line.split(",");

                for (String value : values) {

                    value = value.trim();

                    if (!value.isEmpty()) {
                        numbers.add(value);
                    }
                }
            }

            reader.close();

            if (numbers.isEmpty()) {

                JOptionPane.showMessageDialog(
                        this,"The CSV file contains no numbers."
                );

                return;
            }

            // Put the CSV numbers into the input field.
            inputField.setText(
                    String.join(",", numbers)
            );

            resultArea.append(
                    "CSV loaded: "
                    + file.getName()
                    + "\n"
            );

        } catch (Exception e) {

            JOptionPane.showMessageDialog(
                    this,
                    "Could not load CSV file: "
                    + e.getMessage()
            );
        }
    }

    
     //Sends the selected job to the
     //distributed worker system.
    
    private void submitJob() {

        // Get the selected job.
        String selectedJob = (String) jobType.getSelectedItem();

        // Get the input entered by the user.
        String input = inputField.getText().trim();

        // Check for empty input.
        if (input.isEmpty()) {

            resultArea.append(
                    "Please enter some input.\n"
            );

            return;
        }

        
        // Run the client request in a separate
        // thread so the GUI does not freeze.
         
        new Thread(() -> {

            try {

                // Create the RMI client.
                Client client = new Client();

                int result;

                
                // MAX
                if (selectedJob.equals("MAX")) {

                    int[] numbers = parseNumbers(input);

                    result = client.submitMax(numbers);
                }

               
                // PRIMESUM
                else if (
                        selectedJob.equals("PRIMESUM")
                ) {

                    
                     //PRIMESUM input format:
                     //start,end
                     //Example:
                     //1,1000
                     
                    String[] values = input.split(",");

                    if (values.length != 2) {

                        throw new IllegalArgumentException(
                                "PRIMESUM format must be: start,end"
                        );
                    }

                    int start =
                            Integer.parseInt(
                                    values[0].trim()
                            );

                    int end =
                            Integer.parseInt(
                                    values[1].trim()
                            );

                    result =
                            client.submitPrimeSum(
                                    start,
                                    end
                            );
                }

                
                // PRIMECOUNT
                else {

                    int[] numbers = parseNumbers(input);

                    result = client.submitPrimeCount(numbers);
                }

                /*
                 * Update the GUI using the Swing
                 * Event Dispatch Thread.
                 */
                SwingUtilities.invokeLater(() -> {

                    resultArea.append(
                            selectedJob
                            + " Result: "
                            + result
                            + "\n"
                    );
                });

            } catch (Exception e) {

                
                 //Display any error in the GUI.
                 
                SwingUtilities.invokeLater(() -> {

                    resultArea.append(
                            "Error: "
                            + e.getMessage()
                            + "\n"
                    );
                });
            }

        }).start();
    }

    /**
     * Converts comma-separated numbers into
     * an integer array.
     */
    private int[] parseNumbers(String input) {

        String[] values =
                input.split(",");

        int[] numbers =
                new int[values.length];

        for (int i = 0;
                i < values.length;
                i++) {

            numbers[i] =
                    Integer.parseInt(
                            values[i].trim()
                    );
        }

        return numbers;
    }

    
     //Main method used to start the GUI.
     
    public static void main(String[] args) {

        SwingUtilities.invokeLater(() -> {

            ClientGUI gui = new ClientGUI();

            gui.setVisible(true);
        });
    }
}